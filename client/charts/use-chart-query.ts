/**
 * use-chart-query.ts — Encapsulates chart data query logic.
 *
 * Manages loading, error, and data state for timeseries queries.
 * Provides runQuery callback that fetches data for selected Y columns.
 */

import { useCallback, useState } from "react";
import type { ColumnMappingOption, TimeseriesPoint } from "./chart-api";
import { queryTimeseries } from "./chart-api";
import type { WorkspaceState } from "./chart-prototype.types";

interface UseChartQueryOptions {
  columns: ColumnMappingOption[];
}

export function useChartQuery({ columns }: UseChartQueryOptions) {
  const [data, setData] = useState<TimeseriesPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasQueried, setHasQueried] = useState(false);

  const runQuery = useCallback(
    async (ws: WorkspaceState) => {
      if (ws.yColumnIds.length === 0) {
        setError("Select at least one Y field before running the query.");
        return;
      }

      setLoading(true);
      setError(null);
      setHasQueried(true);
      try {
        const columnMap = new Map(columns.map((c) => [c.column_mapping_id, c]));
        const perColumn = await Promise.all(
          ws.yColumnIds.map(async (columnId) => {
            const columnPoints = await queryTimeseries({
              source_system: ws.sourceSystem || undefined,
              dataset_key: ws.datasetKey || undefined,
              column_mapping_id: columnId,
              date_from: ws.dateFrom || undefined,
              date_to: ws.dateTo || undefined,
              dimension_type: ws.dimensionType || undefined,
              dimension_key: ws.dimensionKey || undefined,
              split_dimensions: ws.splitDimensions,
              aggregation: ws.aggregation as
                | "none"
                | "hourly"
                | "daily"
                | "monthly",
              agg_func: ws.aggFunc as "avg" | "sum" | "min" | "max",
              limit: ws.limit ?? undefined,
            });
            const col = columnMap.get(columnId);
            const label = col
              ? col.column_label || col.semantic_key || col.raw_column
              : `Column ${columnId}`;
            return columnPoints.map((pt) => ({ ...pt, series_label: label }));
          })
        );
        setData(perColumn.flat());
      } catch (e) {
        setError(e instanceof Error ? e.message : "Query failed");
      } finally {
        setLoading(false);
      }
    },
    [columns]
  );

  return { data, loading, error, hasQueried, runQuery };
}
