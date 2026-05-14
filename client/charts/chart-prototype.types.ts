/**
 * Shared type definitions for the chart prototype workspace.
 * Kept separate from `chart-api.ts` to avoid circular imports between the API client
 * and the UI state types.
 */
import type { ChartType } from "./chart-api";

/** Full mutable state of the chart builder workspace (data source, filters, style options). */
export type WorkspaceState = {
  chartType: ChartType;
  xField: string;
  sourceSystem: string;
  datasetKey: string;
  yColumnIds: number[];
  dateFrom: string;
  dateTo: string;
  dimensionType: string;
  dimensionKey: string;
  splitDimensions: boolean;
  sumDimensions: boolean;
  stackSeries: boolean;
  smoothLines: boolean;
  showMarkers: boolean;
  stepLines: boolean;
  aggregation: string;
  aggFunc: string;
  limit: number | null;
};

/**
 * Intermediate series representation produced by `getSeriesDrafts` before being
 * converted to ECharts `series` option objects.
 */
export type SeriesDraft = {
  name: string;
  baseLabel: string;
  points: Array<[string, number]>;
  isTotal: boolean;
};

/** Shape of a single tooltip entry received from ECharts formatter callbacks. */
export type TooltipItem = {
  axisValueLabel?: string;
  axisValue?: string | number;
  marker?: string;
  seriesName?: string;
  value?: unknown;
};