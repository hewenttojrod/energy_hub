/**
 * API client for the timeseries point explorer and raw-record drill-down.
 *  - timeseries/points/                      →  /api/core/charts/timeseries/points/
 *  - timeseries/raw-record/                  →  /api/core/charts/timeseries/raw-record/
 *  - timeseries/raw-records-by-timestamp/    →  /api/core/charts/timeseries/raw-records-by-timestamp/
 *  - column-mappings/                        →  /api/core/charts/column-mappings/
 */
import { fetchWithRetry } from "@/utils/api-fetch";
import { parseJsonResponse } from "@/utils/api-json";
import type { ColumnMappingOption } from "@app-types/api";

export type { ColumnMappingOption };

export const TIMESERIES_POINTS_ENDPOINT = "/api/core/charts/timeseries/points/";
export const TIMESERIES_RAW_RECORD_ENDPOINT = "/api/core/charts/timeseries/raw-record/";
export const TIMESERIES_RAW_RECORDS_BY_TIMESTAMP_ENDPOINT = "/api/core/charts/timeseries/raw-records-by-timestamp/";
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

// ---------------------------------------------------------------------------
// Fetch helpers
// ---------------------------------------------------------------------------

export async function fetchColumnMappings(
  source_system?: string,
  dataset_key?: string
): Promise<ColumnMappingOption[]> {
  const url = new URL(COLUMN_MAPPINGS_ENDPOINT, window.location.origin);
  if (source_system) url.searchParams.set("source_system", source_system);
  if (dataset_key) url.searchParams.set("dataset_key", dataset_key);
  const res = await fetchWithRetry(url.toString());
  return parseJsonResponse<ColumnMappingOption[]>(res);
}

export async function fetchRawRecord(timeseries_point_id: number): Promise<RawRecord> {
  const url = new URL(TIMESERIES_RAW_RECORD_ENDPOINT, window.location.origin);
  url.searchParams.set("timeseries_point_id", String(timeseries_point_id));
  const res = await fetchWithRetry(url.toString());
  return parseJsonResponse<RawRecord>(res);
}

export async function fetchRawRecordsByTimestamp(timeseries_point_id: number): Promise<RawRecord[]> {
  const url = new URL(TIMESERIES_RAW_RECORDS_BY_TIMESTAMP_ENDPOINT, window.location.origin);
  url.searchParams.set("timeseries_point_id", String(timeseries_point_id));
  const res = await fetchWithRetry(url.toString());
  return parseJsonResponse<RawRecord[]>(res);
}
