"""
Parse Power BI "querydata" JSON responses into flat row dictionaries.

─── Power BI response anatomy ───────────────────────────────────────

A typical querydata response:

{
  "results": [{
    "result": {
      "data": {
        "dsr": {
          "DS": [{                              ← DataShape
            "N":  "DS0",
            "PH": [{"DM0": [ ... rows ... ]}],  ← PageHierarchy → DataMembers
            "SH": [{"DM1": [ ... ]}],            ← SubHeaders (matrix only)
            "IC": true
          }]
        }
      }
    }
  }]
}

─── Row formats ─────────────────────────────────────────────────────

**Simple (C-based)**  —  most visuals:

  Row 0 (carries the schema):
    {"S": [{"N":"G0","T":1}, {"N":"M0","T":4}], "C": ["USA", 42]}

  Later rows:
    {"C": ["UK", 10]}                 ← all values present
    {"C": [5], "R": 1}               ← R bitmask: bit 0 → col 0 repeats
    {"C": [1772236800000], "Ø": 6}   ← Ø bitmask: bits 1,2 → those cols null
    {"C": [], "R": 1, "Ø": 6}       ← combined

  For each column position:
    1. R bit set  → copy value from previous row
    2. Ø bit set  → value is None (null)
    3. otherwise  → pop the next value from the packed "C" array

**Matrix (X-based)**  —  pivot-table visuals:

  Row 0:
    {"S": [{"N":"G0","T":1}], "G0": "Country A",
     "X": [{"S":[{"N":"M0","T":4}], "M0": 10}, {"M0": 20}]}

  SH (SubHeaders at DS level) names the cross-tab columns:
    [{"DM1": [{"S":[{"N":"G1","T":1}], "G1":"Missile"}, {"G1":"UAV"}]}]

  Flattened: {"G0":"Country A", "Missile_M0": 10, "UAV_M0": 20}

─── Column naming ───────────────────────────────────────────────────

G0, G1… are group (dimension) columns; M0, M1… are measures.
Friendly names come from the request payload's Select array:
  ["Table.Country", "Sum(Table.Sales)"]  →  G0="Country", M0="Sales"
─────────────────────────────────────────────────────────────────────
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


# ── helpers: extract friendly names from request payload ───────────

def _friendly_name(select_expr: str) -> str:
    """
    'Sum(Table.Column Name)' → 'Column Name'
    'Table.Column Name'      → 'Column Name'
    """
    inner = re.sub(r"^[A-Za-z]+\((.+)\)$", r"\1", select_expr)
    parts = inner.rsplit(".", 1)
    return parts[-1].strip() if len(parts) > 1 else inner.strip()


def build_column_map(request_payload: dict | None) -> dict[str, str]:
    """
    Map internal names (G0, M0, …) to human-readable names extracted from
    the request payload's ``Select`` array.

    Returns a dict like ``{"G0": "Country", "M0": "Sales", ...}``.
    """
    mapping: dict[str, str] = {}
    if not request_payload:
        return mapping

    try:
        queries = request_payload.get("queries", [])
        for q in queries:
            cmds = q.get("Query", {}).get("Commands", [])
            for cmd in cmds:
                sas = cmd.get("SemanticQueryDataShapeCommand", {})
                selects = sas.get("Query", {}).get("Select", [])
                g_idx = m_idx = 0
                for sel in selects:
                    name = sel.get("Name", "")
                    friendly = _friendly_name(name)
                    agg_keys = {"Sum", "Avg", "Count", "Min", "Max",
                                "CountNotNull", "Median"}
                    is_measure = any(
                        k in sel for k in ("Aggregation", "Measure")
                    ) or name.split("(")[0] in agg_keys
                    if is_measure:
                        mapping[f"M{m_idx}"] = friendly
                        m_idx += 1
                    else:
                        mapping[f"G{g_idx}"] = friendly
                        g_idx += 1
    except Exception as exc:
        logger.debug("Could not build column map: %s", exc)
    return mapping


# ── simple (C-based) parser ────────────────────────────────────────

def _parse_simple_rows(
    dm0_rows: list[dict],
    col_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Parse DM0 rows that use the C/R/Ø format."""
    if not dm0_rows:
        return []

    first = dm0_rows[0]
    schema: list[dict] = first.get("S", [])
    if not schema:
        return []

    col_keys = [s["N"] for s in schema]
    col_names = [col_map.get(k, k) for k in col_keys]
    num_cols = len(col_keys)

    prev_values: list[Any] = [None] * num_cols
    records: list[dict[str, Any]] = []

    for row in dm0_rows:
        cell_values = list(row.get("C", []))
        repeat_mask = row.get("R", 0)
        null_mask = row.get("\u00d8", 0)  # Ø

        current: list[Any] = []
        c_idx = 0
        for col_idx in range(num_cols):
            if (repeat_mask >> col_idx) & 1:
                current.append(prev_values[col_idx])
            elif (null_mask >> col_idx) & 1:
                current.append(None)
            else:
                if c_idx < len(cell_values):
                    current.append(cell_values[c_idx])
                    c_idx += 1
                else:
                    current.append(None)

        prev_values = current
        records.append(dict(zip(col_names, current)))

    return records


# ── matrix (X-based) parser ────────────────────────────────────────

def _parse_matrix_rows(
    dm0_rows: list[dict],
    sub_headers: list[dict],
    col_map: dict[str, str],
) -> list[dict[str, Any]]:
    """Parse DM0 rows that use the G0/X/SH cross-tab format."""
    if not dm0_rows:
        return []

    # Resolve sub-header labels from SH → DM1
    sh_labels: list[str] = []
    for sh_entry in sub_headers:
        for key in sorted(sh_entry.keys()):  # DM1, DM2, …
            for member in sh_entry[key]:
                label_key = next(
                    (k for k in member if k not in ("S", "R", "Ø", "X", "I")),
                    None,
                )
                if label_key and isinstance(member.get(label_key), str):
                    sh_labels.append(member[label_key])

    first = dm0_rows[0]
    group_schema = first.get("S", [])
    group_keys = [s["N"] for s in group_schema]
    group_names = [col_map.get(k, k) for k in group_keys]

    # Determine measure keys from the first X entry that has S
    measure_keys: list[str] = []
    for row in dm0_rows:
        for x_entry in row.get("X", []):
            if "S" in x_entry:
                measure_keys = [s["N"] for s in x_entry["S"]]
                break
        if measure_keys:
            break

    records: list[dict[str, Any]] = []
    prev_group: dict[str, Any] = {}
    prev_x_values: dict[tuple[int, str], Any] = {}

    for row in dm0_rows:
        record: dict[str, Any] = {}

        for gk, gn in zip(group_keys, group_names):
            if gk in row:
                record[gn] = row[gk]
                prev_group[gn] = row[gk]
            else:
                record[gn] = prev_group.get(gn)

        x_entries = row.get("X", [])
        offset = 0
        for xi, x_entry in enumerate(x_entries):
            actual_idx = x_entry.get("I", xi if xi == 0 and "I" not in x_entries[0] else None)
            if actual_idx is None:
                actual_idx = offset
            offset = actual_idx + 1

            sh_label = sh_labels[actual_idx] if actual_idx < len(sh_labels) else f"col{actual_idx}"
            x_repeat = x_entry.get("R", 0)

            for mk in measure_keys:
                measure_name = col_map.get(mk, mk)
                col_label = f"{sh_label} - {measure_name}" if sh_labels else measure_name

                if mk in x_entry:
                    val = x_entry[mk]
                    prev_x_values[(actual_idx, mk)] = val
                elif x_repeat and (x_repeat >> measure_keys.index(mk)) & 1:
                    val = prev_x_values.get((actual_idx, mk))
                else:
                    val = None
                record[col_label] = val

        # Fill any sub-header columns not present in this row's X
        for si, sh_label in enumerate(sh_labels):
            for mk in measure_keys:
                measure_name = col_map.get(mk, mk)
                col_label = f"{sh_label} - {measure_name}"
                if col_label not in record:
                    record[col_label] = None

        records.append(record)

    return records


# ── top-level API ──────────────────────────────────────────────────

def parse_response(
    response_json: dict,
    request_payload: dict | None = None,
) -> list[list[dict[str, Any]]]:
    """
    Parse a full Power BI querydata response.

    Returns a list of datasets (one per DataShape).  Each dataset is a list
    of dictionaries suitable for conversion to a DataFrame.
    """
    col_map = build_column_map(request_payload)
    datasets: list[list[dict[str, Any]]] = []

    for result_entry in response_json.get("results", []):
        dsr = (
            result_entry
            .get("result", {})
            .get("data", {})
            .get("dsr", {})
        )
        for ds in dsr.get("DS", []):
            sub_headers = ds.get("SH", [])

            for ph_entry in ds.get("PH", []):
                dm0 = ph_entry.get("DM0", [])
                if not dm0:
                    continue

                first_row = dm0[0]
                is_matrix = "X" in first_row

                if is_matrix:
                    records = _parse_matrix_rows(dm0, sub_headers, col_map)
                else:
                    records = _parse_simple_rows(dm0, col_map)

                if records:
                    datasets.append(records)
                    logger.info(
                        "Parsed %d rows × %d cols (%s format)",
                        len(records),
                        len(records[0]),
                        "matrix" if is_matrix else "simple",
                    )

    if not datasets:
        logger.debug("No parseable datasets in response")

    return datasets


def try_parse(
    response_json: dict | None,
    request_payload: dict | None = None,
) -> list[list[dict[str, Any]]]:
    """Safe wrapper that returns ``[]`` on failure."""
    if response_json is None:
        return []
    try:
        return parse_response(response_json, request_payload)
    except Exception as exc:
        logger.warning("Failed to parse response: %s", exc)
        return []
