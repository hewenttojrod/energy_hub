/**
 * Static constants and default workspace state for the chart prototype.
 *
 * Centralising these here avoids magic strings scattered across components and
 * makes it easy to extend chart type/aggregation options in one place.
 */
import type { ChartType } from "./chart-api";
import type { WorkspaceState } from "./chart-prototype.types";

/** Supported chart types available in the type picker. */
export const CHART_TYPES: { value: ChartType; label: string }[] = [
  { value: "line", label: "Line" },
  { value: "bar", label: "Bar" },
  { value: "area", label: "Area" },
  { value: "scatter", label: "Scatter" },
];

/** Temporal aggregation levels available in the aggregation picker. */
export const AGGREGATIONS = [
  { value: "none", label: "Raw (no aggregation)" },
  { value: "hourly", label: "Hourly" },
  { value: "daily", label: "Daily" },
  { value: "monthly", label: "Monthly" },
] as const;

/** Aggregation functions applied when a non-"none" aggregation level is selected. */
export const AGG_FUNCS = [
  { value: "avg", label: "Average" },
  { value: "sum", label: "Sum" },
  { value: "min", label: "Minimum" },
  { value: "max", label: "Maximum" },
] as const;

/** Default row limit applied when the user has not entered a custom value. */
export const DEFAULT_LIMIT = 2000;
/** The x-axis field is always the UTC timestamp; not configurable by the user. */
export const LOCKED_X_FIELD = "ts_utc";
/** Column `base_data_type` values that can be plotted as numeric y-axis series. */
export const PLOTTABLE_BASE_TYPES = new Set(["float", "int"]);

export const DEFAULT_WORKSPACE: WorkspaceState = {
  chartType: "line",
  xField: LOCKED_X_FIELD,
  sourceSystem: "",
  datasetKey: "",
  yColumnIds: [],
  dateFrom: "",
  dateTo: "",
  dimensionType: "",
  dimensionKey: "",
  splitDimensions: true,
  sumDimensions: false,
  stackSeries: false,
  smoothLines: true,
  showMarkers: false,
  stepLines: false,
  aggregation: "none",
  aggFunc: "sum",
  limit: DEFAULT_LIMIT,
};