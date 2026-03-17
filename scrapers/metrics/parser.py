"""
Parse Power BI "querydata" JSON responses into flat row dictionaries.
(No changes from original — no config dependency.)
"""

from __future__ import annotations

import logging
import re
from typing import Any

logger = logging.getLogger(__name__)


def _friendly_name(select_expr: str) -> str:
    inner = re.sub(r"^[A-Za-z]+\((.+)\)$", r"\1", select_expr)
    parts = inner.rsplit(".", 1)
    return parts[-1].strip() if len(parts) > 1 else inner.strip()


def build_column_map(request_payload: dict | None) -> dict[str, str]:
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
                    agg_keys = {"Sum", "Avg", "Count", "Min", "Max", "CountNotNull", "Median"}
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


def _parse_simple_rows(dm0_rows: list[dict], col_map: dict[str, str]) -> list[dict[str, Any]]:
    if not dm0_rows:
        return []

    first = dm0_rows[0]
    schema: list[dict] = first.get("S", [])
    if not schema:
        return []

    col_keys = [s.get("N", f"col{i}") for i, s in enumerate(schema)]
    col_names = [col_map.get(k, k) for k in col_keys]
    num_cols = len(col_keys)

    prev_values: list[Any] = [None] * num_cols
    records: list[dict[str, Any]] = []

    for row in dm0_rows:
        cell_values = list(row.get("C", []))
        repeat_mask = row.get("R", 0)
        null_mask = row.get("\u00d8", 0)

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


def _parse_matrix_rows(
    dm0_rows: list[dict],
    sub_headers: list[dict],
    col_map: dict[str, str],
) -> list[dict[str, Any]]:
    if not dm0_rows:
        return []

    sh_labels: list[str] = []
    for sh_entry in sub_headers:
        for key in sorted(sh_entry.keys(), key=lambda k: int(k[2:]) if k[2:].isdigit() else 0):
            for member in sh_entry[key]:
                label_key = next(
                    (k for k in member if k not in ("S", "R", "Ø", "X", "I")), None
                )
                if label_key and isinstance(member.get(label_key), str):
                    sh_labels.append(member[label_key])

    first = dm0_rows[0]
    group_schema = first.get("S", [])
    group_keys = [s["N"] for s in group_schema]
    group_names = [col_map.get(k, k) for k in group_keys]

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
            actual_idx = x_entry.get("I", xi)
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

        for si, sh_label in enumerate(sh_labels):
            for mk in measure_keys:
                measure_name = col_map.get(mk, mk)
                col_label = f"{sh_label} - {measure_name}"
                if col_label not in record:
                    record[col_label] = None

        records.append(record)

    return records


def parse_response(
    response_json: dict,
    request_payload: dict | None = None,
) -> list[list[dict[str, Any]]]:
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
                is_matrix = "X" in dm0[0]
                if is_matrix:
                    records = _parse_matrix_rows(dm0, sub_headers, col_map)
                else:
                    records = _parse_simple_rows(dm0, col_map)
                if records:
                    datasets.append(records)
                    logger.info(
                        "Parsed %d rows × %d cols (%s format)",
                        len(records), len(records[0]),
                        "matrix" if is_matrix else "simple",
                    )

    return datasets


def try_parse(
    response_json: dict | None,
    request_payload: dict | None = None,
) -> list[list[dict[str, Any]]]:
    if response_json is None:
        return []
    try:
        return parse_response(response_json, request_payload)
    except Exception as exc:
        logger.warning("Failed to parse response: %s", exc)
        return []
