/**
 * Pure utility functions for transforming timeseries API data into ECharts option objects.
 * Contains no React state — all functions are side-effect-free and independently testable.
 */
import * as echarts from "echarts";
import type { ColumnMappingOption, TimeseriesPoint } from "./chart-api";
import { PLOTTABLE_BASE_TYPES } from "./chart-prototype.constants";
import type { SeriesDraft, TooltipItem, WorkspaceState } from "./chart-prototype.types";

/**
 * Returns true when the given column mapping has a numeric `base_data_type`
 * (i.e. "float" or "int") and is therefore eligible to be plotted on the y-axis.
 */
export function isValueColumn(column: ColumnMappingOption): boolean {
  return Boolean(column.base_data_type && PLOTTABLE_BASE_TYPES.has(column.base_data_type));
}

function formatDimensionLabel(point: TimeseriesPoint): string {
  if (point.dimension_type && point.dimension_key) {
    return `${point.dimension_type}: ${point.dimension_key}`;
  }
  if (point.dimension_key) {
    return point.dimension_key;
  }
  if (point.dimension_type) {
    return point.dimension_type;
  }
  return "Unspecified";
}

function formatTooltipNumber(value: number): string {
  return Number.isFinite(value)
    ? value.toLocaleString(undefined, { maximumFractionDigits: 4 })
    : "0";
}

function getSeriesDrafts(data: TimeseriesPoint[], ws: WorkspaceState): SeriesDraft[] {
  const seriesMap = new Map<string, Map<string, number>>();
  const seriesBaseMap = new Map<string, string>();
  const totalMap = new Map<string, Map<string, number>>();
  const dimensionVariants = new Map<string, Set<string>>();

  for (const point of data) {
    const baseLabel = point.series_label ?? point.semantic_key ?? "value";
    const dimensionLabel = ws.splitDimensions ? formatDimensionLabel(point) : "";
    const seriesName = dimensionLabel ? `${baseLabel} [${dimensionLabel}]` : baseLabel;

    if (!seriesMap.has(seriesName)) {
      seriesMap.set(seriesName, new Map<string, number>());
      seriesBaseMap.set(seriesName, baseLabel);
    }

    const pointsByTime = seriesMap.get(seriesName);
    if (!pointsByTime) {
      continue;
    }
    pointsByTime.set(point.ts_utc, (pointsByTime.get(point.ts_utc) ?? 0) + point.value_num);

    if (dimensionLabel) {
      if (!dimensionVariants.has(baseLabel)) {
        dimensionVariants.set(baseLabel, new Set<string>());
      }
      dimensionVariants.get(baseLabel)?.add(seriesName);

      if (ws.sumDimensions) {
        const totalName = `${baseLabel} [Total]`;
        if (!totalMap.has(totalName)) {
          totalMap.set(totalName, new Map<string, number>());
        }
        const totalsByTime = totalMap.get(totalName);
        if (totalsByTime) {
          totalsByTime.set(point.ts_utc, (totalsByTime.get(point.ts_utc) ?? 0) + point.value_num);
        }
      }
    }
  }

  const drafts: SeriesDraft[] = Array.from(seriesMap.entries()).map(([name, pointMap]) => ({
    name,
    baseLabel: seriesBaseMap.get(name) ?? name,
    isTotal: false,
    points: Array.from(pointMap.entries()).sort((left, right) => left[0].localeCompare(right[0])),
  }));

  if (ws.sumDimensions) {
    totalMap.forEach((pointMap, totalName) => {
      const baseLabel = totalName.replace(/ \[Total\]$/, "");
      if ((dimensionVariants.get(baseLabel)?.size ?? 0) < 2) {
        return;
      }
      drafts.push({
        name: totalName,
        baseLabel,
        isTotal: true,
        points: Array.from(pointMap.entries()).sort((left, right) => left[0].localeCompare(right[0])),
      });
    });
  }

  return drafts;
}

export function buildEChartsOption(
  data: TimeseriesPoint[],
  ws: WorkspaceState,
  columns: ColumnMappingOption[]
): echarts.EChartsOption {
  const seriesDrafts = getSeriesDrafts(data, ws);
  const supportsLineControls = ws.chartType === "line" || ws.chartType === "area";
  const baseType = ws.chartType === "area" ? "line" : ws.chartType;

  const series: echarts.SeriesOption[] = seriesDrafts.map((draft) => ({
    name: draft.name,
    type: draft.isTotal ? "line" : (baseType as "line" | "bar" | "scatter"),
    data: draft.points.map(([x, y]) => [x, y]),
    stack: !draft.isTotal && ws.stackSeries && (baseType === "bar" || ws.chartType === "area")
      ? draft.baseLabel
      : undefined,
    areaStyle: !draft.isTotal && ws.chartType === "area" ? { opacity: 0.2 } : undefined,
    lineStyle: draft.isTotal ? { width: 3, type: "dashed" } : undefined,
    emphasis: { focus: "series" },
    ...(supportsLineControls
      ? {
          smooth: ws.smoothLines,
          step: ws.stepLines ? "middle" : undefined,
          showSymbol: ws.showMarkers,
          symbol: ws.showMarkers ? "circle" : "none",
        }
      : {}),
  }));

  const firstColId = ws.yColumnIds[0];
  const col = columns.find((c) => c.column_mapping_id === firstColId);
  const yAxisName = ws.yColumnIds.length > 1
    ? "Selected values"
    : col
      ? `${col.column_label || col.raw_column}${col.unit_name ? ` (${col.unit_name})` : ""}`
      : "Value";

  return {
    backgroundColor: "transparent",
    animationDuration: 250,
    tooltip: {
      trigger: "axis",
      axisPointer: { type: "cross" },
      formatter: (params: unknown) => {
        const items = (Array.isArray(params) ? params : [params]) as TooltipItem[];
        if (items.length === 0) {
          return "";
        }

        const axisValueLabel = items[0]?.axisValueLabel || String(items[0]?.axisValue ?? "");
        const itemLines = items.map((item) => {
          const rawValue = Array.isArray(item.value)
            ? Number(item.value[1] ?? 0)
            : Number(item.value ?? 0);
          return `${item.marker ?? ""}${item.seriesName ?? "Series"}: ${formatTooltipNumber(rawValue)}`;
        });

        const hasTotalSeries = items.some((item) => String(item.seriesName ?? "").endsWith("[Total]"));
        if (!hasTotalSeries && items.length > 1) {
          const total = items.reduce((sum, item) => {
            const rawValue = Array.isArray(item.value)
              ? Number(item.value[1] ?? 0)
              : Number(item.value ?? 0);
            return sum + rawValue;
          }, 0);
          itemLines.push(`<span style="font-weight:600">Total: ${formatTooltipNumber(total)}</span>`);
        }

        return [axisValueLabel, ...itemLines].join("<br/>");
      },
    },
    legend: { top: 8, type: "scroll" },
    grid: { left: 60, right: 24, top: 48, bottom: 60 },
    xAxis: {
      type: "time",
      name: ws.xField,
      nameLocation: "middle",
      nameGap: 34,
      axisLabel: { rotate: 30, fontSize: 11 },
    },
    yAxis: {
      type: "value",
      name: yAxisName,
      nameLocation: "middle",
      nameGap: 40,
      nameTextStyle: { fontSize: 11 },
    },
    dataZoom: [
      { type: "inside", start: 0, end: 100 },
      { type: "slider", start: 0, end: 100, height: 22, bottom: 8 },
    ],
    toolbox: {
      right: 12,
      feature: {
        dataZoom: {},
        restore: {},
        saveAsImage: { pixelRatio: 2 },
      },
    },
    series,
  };
}