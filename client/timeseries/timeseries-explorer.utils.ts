/**
 * Pure utility functions and static column definitions for `TimeseriesExplorer`.
 * Kept separate from the component to make individual helpers independently testable.
 */
import type { ColumnDef } from "@app-types/api";

import type { ColumnMappingOption, TimeseriesPointRow } from "./timeseries-api";

/**
 * Formats a `value_json` object (multi-key value bag) into a compact display string.
 * Shows `key: value` pairs. Caps at 3 pairs and appends "..." when there are more.
 * Returns "-" for an empty object.
 */
export function formatValueJson(valueJson: Record<string, unknown>): string {
  const entries = Object.entries(valueJson);
  if (entries.length === 0) return "-";
  if (entries.length === 1) {
    const [key, value] = entries[0];
    return `${key}: ${String(value)}`;
  }
  return entries
    .slice(0, 3)
    .map(([key, value]) => `${key}: ${String(value)}`)
    .join("  |  ")
    .concat(entries.length > 3 ? "  ..." : "");
}

/** Formats an ISO timestamp string to locale date-time; returns the raw string on parse failure. */
export function formatTs(value: string): string {
  try {
    return new Date(value).toLocaleString();
  } catch {
    return value;
  }
}

/**
 * Groups a flat array of column mappings by `dataset_key` for sectioned display
 * in the column-picker UI.
 */
export function groupMappingsByDataset(
  mappings: ColumnMappingOption[]
): Record<string, ColumnMappingOption[]> {
  const groups: Record<string, ColumnMappingOption[]> = {};
  for (const mapping of mappings) {
    (groups[mapping.dataset_key] ??= []).push(mapping);
  }
  return groups;
}

type BuildAppliedParamsArgs = {
  selectedIds: Set<number>;
  sourceSystem: string;
  datasetKey: string;
  dateFrom: string;
  dateTo: string;
  limit: string;
};

/**
 * Builds the query parameter object passed to the `DataGrid` after the user clicks "Apply".
 *
 * Priority logic:
 * - If any column IDs are selected, they are sent as `column_mapping_ids` (comma-separated)
 *   and source/dataset filters are omitted (the backend already knows which columns belong
 *   to which source/dataset).
 * - If no columns are selected, falls back to `source_system` + `dataset_key` filters.
 * - Date range, limit, and ordering params are always included when non-empty.
 */
export function buildAppliedParams({
  selectedIds,
  sourceSystem,
  datasetKey,
  dateFrom,
  dateTo,
  limit,
}: BuildAppliedParamsArgs): Record<string, string> {
  const params: Record<string, string> = {};

  if (selectedIds.size > 0) {
    params.column_mapping_ids = [...selectedIds].join(",");
  } else {
    if (sourceSystem) params.source_system = sourceSystem;
    if (datasetKey) params.dataset_key = datasetKey;
  }

  if (dateFrom) params.date_from = dateFrom;
  if (dateTo) params.date_to = dateTo;
  params.limit = limit || "500";

  return params;
}

export const TIMESERIES_COLUMNS: ColumnDef<TimeseriesPointRow>[] = [
  {
    key: "ts_utc",
    label: "Timestamp (UTC)",
    width: "170px",
    sortable: true,
    render: (value) => formatTs(String(value)),
  },
  { key: "column_label", label: "Column", width: "160px", sortable: true },
  { key: "semantic_key", label: "Semantic Key", width: "140px", sortable: false },
  { key: "unit_name", label: "Unit", width: "80px" },
  {
    key: "value_json",
    label: "Values",
    render: (value) => formatValueJson(value as Record<string, unknown>),
  },
  { key: "source_file_name", label: "Source File", width: "220px", sortable: true },
];
