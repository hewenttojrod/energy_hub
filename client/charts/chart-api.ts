/**
 * API client for core chart endpoints.
 *  - chart_definition CRUD  →  /api/core/charts/definitions/
 *  - column mapping list    →  /api/core/charts/column-mappings/
 *  - dimension list         →  /api/core/charts/dimensions/
 *  - timeseries query       →  /api/core/charts/timeseries/
 */
import { fetchWithRetry } from "@/utils/api-fetch";

export const CHART_BASE = "/api/core/charts";

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

export type ChartType = "line" | "bar" | "scatter" | "area" | "pie" | "heatmap";
export type Aggregation = "none" | "hourly" | "daily" | "monthly";
export type AggFunc = "avg" | "sum" | "min" | "max";

export type ChartDefinition = {
  chart_definition_id: number;
  name: string;
  description: string;
  chart_type: ChartType;
  config_json: ChartConfig;
  last_data_json: Record<string, unknown>;
  is_pinned: boolean;
  created_at: string;
  updated_at: string;
};

export type ChartConfig = {
  chartType?: ChartType;
  xField?: string;
  yColumnIds?: number[];
  sourceSystem?: string;
  datasetKey?: string;
  dimensionType?: string;
  dimensionKey?: string;
  aggFunc?: AggFunc;
  splitDimensions?: boolean;
  sumDimensions?: boolean;
  stackSeries?: boolean;
  smoothLines?: boolean;
  showMarkers?: boolean;
  stepLines?: boolean;
  source_system?: string;
  dataset_key?: string;
  column_mapping_ids?: number[];
  date_from?: string;
  date_to?: string;
  dimension_type?: string;
  dimension_key?: string;
  aggregation?: Aggregation;
  agg_func?: AggFunc;
  limit?: number | null;
  style_overrides?: Record<string, unknown>;
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

export type DimensionOption = {
  dimension_type: string;
  dimension_key: string;
};

export type TimeseriesPoint = {
  ts_utc: string;
  value_num: number;
  dimension_type: string | null;
  dimension_key: string | null;
  semantic_key: string | null;
  unit_name: string | null;
  series_label?: string;
};

// ---------------------------------------------------------------------------
// Chart definition CRUD
// ---------------------------------------------------------------------------

async function parseJson<T>(res: Response): Promise<T> {
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
  return (await res.json()) as T;
}

export async function listChartDefinitions(): Promise<ChartDefinition[]> {
  const res = await fetchWithRetry(`${CHART_BASE}/definitions/`);
  return parseJson(res);
}

export async function saveChartDefinition(
  payload: Omit<ChartDefinition, "chart_definition_id" | "created_at" | "updated_at">
): Promise<ChartDefinition> {
  const res = await fetchWithRetry(`${CHART_BASE}/definitions/`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJson(res);
}

export async function updateChartDefinition(
  id: number,
  payload: Omit<ChartDefinition, "chart_definition_id" | "created_at" | "updated_at">
): Promise<ChartDefinition> {
  const res = await fetchWithRetry(`${CHART_BASE}/definitions/${id}/`, {
    method: "PUT",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJson(res);
}

export async function deleteChartDefinition(id: number): Promise<void> {
  const res = await fetchWithRetry(`${CHART_BASE}/definitions/${id}/`, { method: "DELETE" });
  if (!res.ok) throw new Error(`${res.status} ${res.statusText}`);
}

// ---------------------------------------------------------------------------
// Discovery helpers
// ---------------------------------------------------------------------------

export async function listColumnMappings(
  sourceSystem?: string,
  datasetKey?: string
): Promise<ColumnMappingOption[]> {
  const params = new URLSearchParams();
  if (sourceSystem) params.set("source_system", sourceSystem);
  if (datasetKey) params.set("dataset_key", datasetKey);
  const res = await fetchWithRetry(`${CHART_BASE}/column-mappings/?${params.toString()}`);
  return parseJson(res);
}

export async function listDimensions(
  sourceSystem?: string,
  datasetKey?: string
): Promise<DimensionOption[]> {
  const params = new URLSearchParams();
  if (sourceSystem) params.set("source_system", sourceSystem);
  if (datasetKey) params.set("dataset_key", datasetKey);
  const res = await fetchWithRetry(`${CHART_BASE}/dimensions/?${params.toString()}`);
  return parseJson(res);
}

// ---------------------------------------------------------------------------
// Timeseries query
// ---------------------------------------------------------------------------

export type TimeseriesQueryParams = {
  column_mapping_id?: number;
  source_system?: string;
  dataset_key?: string;
  semantic_key?: string;
  date_from?: string;
  date_to?: string;
  dimension_type?: string;
  dimension_key?: string;
  split_dimensions?: boolean;
  aggregation?: Aggregation;
  agg_func?: AggFunc;
  limit?: number;
};

export async function queryTimeseries(
  params: TimeseriesQueryParams
): Promise<TimeseriesPoint[]> {
  const q = new URLSearchParams();
  Object.entries(params).forEach(([k, v]) => {
    if (v !== undefined && v !== null && v !== "") q.set(k, String(v));
  });
  const res = await fetchWithRetry(`${CHART_BASE}/timeseries/?${q.toString()}`);
  return parseJson(res);
}
