/**
 * API client for the timeseries point explorer and raw-record drill-down.
 *  - timeseries/points/      →  /api/core/charts/timeseries/points/
 *  - timeseries/raw-record/  →  /api/core/charts/timeseries/raw-record/
 *  - column-mappings/        →  /api/core/charts/column-mappings/
 */
import { fetchWithRetry } from "@/utils/api-fetch";

export const TIMESERIES_POINTS_ENDPOINT = "/api/core/charts/timeseries/points/";
export const TIMESERIES_RAW_RECORD_ENDPOINT = "/api/core/charts/timeseries/raw-record/";
export const COLUMN_MAPPINGS_ENDPOINT = "/api/core/charts/column-mappings/";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type TimeseriesPointRow = {
  timeseries_point_id: number;
  ts_utc: string;
  column_mapping_id: number | null;
  column_label: string | null;
  semantic_key: string | null;
  unit_name: string | null;
  value_json: Record<string, unknown>;
  quality_flag: string | null;
  source_file_id: number;
  source_file_name: string;
};

export type RawRecord = {
  raw_record_id: number;
  row_number: number;
  row_payload_json: Record<string, unknown>;
  source_file_id: number;
  source_file_name: string;
};

export type ColumnMappingOption = {
  column_mapping_id: number;
  source_system: string;
  dataset_key: string;
  raw_column: string;
  semantic_key: string;
  column_label: string;
  unit_name: string | null;
  base_data_type: string | null;
};

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

async function parseJson<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

export async function fetchColumnMappings(
  source_system?: string,
  dataset_key?: string
): Promise<ColumnMappingOption[]> {
  const url = new URL(COLUMN_MAPPINGS_ENDPOINT, window.location.origin);
  if (source_system) url.searchParams.set("source_system", source_system);
  if (dataset_key) url.searchParams.set("dataset_key", dataset_key);
  const res = await fetchWithRetry(url.toString());
  return parseJson<ColumnMappingOption[]>(res);
}

export async function fetchRawRecord(timeseries_point_id: number): Promise<RawRecord> {
  const url = new URL(TIMESERIES_RAW_RECORD_ENDPOINT, window.location.origin);
  url.searchParams.set("timeseries_point_id", String(timeseries_point_id));
  const res = await fetchWithRetry(url.toString());
  return parseJson<RawRecord>(res);
}
