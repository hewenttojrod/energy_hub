/**
 * Chart Prototype Page — interactive chart builder backed by ECharts.
 *
 * Left panel  : data source + filter controls, save/load workspace
 * Right panel : live ECharts render
 *
 * Data comes from core timeseries_point via /api/core/charts/timeseries/.
 * Chart configs are persisted to core_chart_definition via /api/core/charts/definitions/.
 */

import { useCallback, useEffect, useRef, useState } from "react";
import * as echarts from "echarts";

import type {
  ChartConfig,
  ChartDefinition,
  ChartType,
  ColumnMappingOption,
  DimensionOption,
  TimeseriesPoint,
} from "./chart-api";
import {
  deleteChartDefinition,
  listChartDefinitions,
  listColumnMappings,
  listDimensions,
  queryTimeseries,
  saveChartDefinition,
  updateChartDefinition,
} from "./chart-api";

// ---------------------------------------------------------------------------
// Constants
// ---------------------------------------------------------------------------

const CHART_TYPES: { value: ChartType; label: string }[] = [
  { value: "line", label: "Line" },
  { value: "bar", label: "Bar" },
  { value: "area", label: "Area" },
  { value: "scatter", label: "Scatter" },
];

const AGGREGATIONS = [
  { value: "none", label: "Raw (no aggregation)" },
  { value: "hourly", label: "Hourly" },
  { value: "daily", label: "Daily" },
  { value: "monthly", label: "Monthly" },
] as const;

const AGG_FUNCS = [
  { value: "avg", label: "Average" },
  { value: "sum", label: "Sum" },
  { value: "min", label: "Minimum" },
  { value: "max", label: "Maximum" },
] as const;

const DEFAULT_LIMIT = 2000;
const LOCKED_X_FIELD = "ts_utc";
const PLOTTABLE_BASE_TYPES = new Set(["float", "int"]);

// ---------------------------------------------------------------------------
// Types
// ---------------------------------------------------------------------------

type WorkspaceState = {
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

const DEFAULT_WORKSPACE: WorkspaceState = {
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

type SeriesDraft = {
  name: string;
  baseLabel: string;
  points: Array<[string, number]>;
  isTotal: boolean;
};

type TooltipItem = {
  axisValueLabel?: string;
  axisValue?: string | number;
  marker?: string;
  seriesName?: string;
  value?: unknown;
};

function isValueColumn(column: ColumnMappingOption): boolean {
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

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

function buildEChartsOption(
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

// ---------------------------------------------------------------------------
// Component
// ---------------------------------------------------------------------------

export default function ChartPrototypePage() {
  // -- workspace (config) state
  const [ws, setWs] = useState<WorkspaceState>(DEFAULT_WORKSPACE);
  const setWsField = <K extends keyof WorkspaceState>(k: K, v: WorkspaceState[K]) =>
    setWs((prev) => ({ ...prev, [k]: v }));

  // -- data
  const [data, setData] = useState<TimeseriesPoint[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasQueried, setHasQueried] = useState(false);

  // -- options from API
  const [columns, setColumns] = useState<ColumnMappingOption[]>([]);
  const [dimensions, setDimensions] = useState<DimensionOption[]>([]);
  const [savedCharts, setSavedCharts] = useState<ChartDefinition[]>([]);

  // -- save state
  const [saveName, setSaveName] = useState("");
  const [saveDesc, setSaveDesc] = useState("");
  const [activeChartId, setActiveChartId] = useState<number | null>(null);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);

  // -- derived: distinct source systems and dataset keys
  const sourceSystems = [...new Set(columns.map((c) => c.source_system))].sort();
  const datasetKeys = [
    ...new Set(
      columns
        .filter((c) => !ws.sourceSystem || c.source_system === ws.sourceSystem)
        .map((c) => c.dataset_key)
    ),
  ].sort();
  const filteredColumns = columns.filter(
    (c) =>
      (!ws.sourceSystem || c.source_system === ws.sourceSystem) &&
      (!ws.datasetKey || c.dataset_key === ws.datasetKey)
  );
  const valueColumns = filteredColumns.filter(isValueColumn);
  const filteredDimTypes = [...new Set(dimensions.map((d) => d.dimension_type))].sort();
  const filteredDimKeys = dimensions
    .filter((d) => !ws.dimensionType || d.dimension_type === ws.dimensionType)
    .map((d) => d.dimension_key)
    .sort();

  const showNoDataHint = hasQueried && !loading && !error && data.length === 0;

  // -- ECharts DOM ref
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<echarts.ECharts | null>(null);

  // -- Bootstrap: load columns + saved charts on mount
  useEffect(() => {
    listColumnMappings().then(setColumns).catch(() => {});
    listChartDefinitions().then(setSavedCharts).catch(() => {});
  }, []);

  // -- Refresh dimensions when source+dataset filter changes
  useEffect(() => {
    listDimensions(ws.sourceSystem || undefined, ws.datasetKey || undefined)
      .then(setDimensions)
      .catch(() => {});
  }, [ws.sourceSystem, ws.datasetKey]);

  // -- Init ECharts instance
  useEffect(() => {
    if (!chartRef.current) return;
    const instance = echarts.init(chartRef.current, "light");
    chartInstance.current = instance;
    const handleResize = () => instance.resize();
    window.addEventListener("resize", handleResize);
    return () => {
      window.removeEventListener("resize", handleResize);
      instance.dispose();
      chartInstance.current = null;
    };
  }, []);

  // -- Re-render chart when data or config changes
  useEffect(() => {
    if (!chartInstance.current) return;
    if (data.length === 0) {
      chartInstance.current.clear();
      return;
    }
    const option = buildEChartsOption(data, ws, columns);
    chartInstance.current.setOption(option, true);
  }, [data, ws, columns]);

  // -- Fetch data
  const runQuery = useCallback(async () => {
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
            aggregation: ws.aggregation as "none" | "hourly" | "daily" | "monthly",
            agg_func: ws.aggFunc as "avg" | "sum" | "min" | "max",
            limit: ws.limit ?? undefined,
          });
          const col = columnMap.get(columnId);
          const label = col
            ? (col.column_label || col.semantic_key || col.raw_column)
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
  }, [columns, ws]);

  // -- Save workspace
  const handleSave = async () => {
    if (!saveName.trim()) return;
    const payload = {
      name: saveName.trim(),
      description: saveDesc.trim(),
      chart_type: ws.chartType,
      config_json: ws as ChartConfig,
      last_data_json: {},
      is_pinned: false,
    };
    try {
      let saved: ChartDefinition;
      if (activeChartId) {
        saved = await updateChartDefinition(activeChartId, payload);
        setSavedCharts((prev) =>
          prev.map((c) => (c.chart_definition_id === activeChartId ? saved : c))
        );
      } else {
        saved = await saveChartDefinition(payload);
        setSavedCharts((prev) => [saved, ...prev]);
      }
      setActiveChartId(saved.chart_definition_id);
      setSaveStatus("Saved.");
      setTimeout(() => setSaveStatus(null), 2000);
    } catch {
      setSaveStatus("Save failed.");
    }
  };

  // -- Load a saved workspace
  const handleLoad = (chart: ChartDefinition) => {
    const cfg = chart.config_json as Partial<WorkspaceState>;
    setWs({
      ...DEFAULT_WORKSPACE,
      ...cfg,
      xField: LOCKED_X_FIELD,
      chartType: chart.chart_type as ChartType,
    });
    setSaveName(chart.name);
    setSaveDesc(chart.description);
    setActiveChartId(chart.chart_definition_id);
    setData([]);
  };

  // -- Delete a saved workspace
  const handleDelete = async (id: number) => {
    try {
      await deleteChartDefinition(id);
      setSavedCharts((prev) => prev.filter((c) => c.chart_definition_id !== id));
      if (activeChartId === id) {
        setActiveChartId(null);
        setSaveName("");
        setSaveDesc("");
      }
    } catch {
      // silent – could surface error toast
    }
  };

  // -- Toggle column selection (multi-select)
  const toggleYColumn = (id: number) => {
    setWsField(
      "yColumnIds",
      ws.yColumnIds.includes(id)
        ? ws.yColumnIds.filter((x) => x !== id)
        : [...ws.yColumnIds, id]
    );
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50 dark:bg-slate-900">
      {/* ------------------------------------------------------------------ */}
      {/* LEFT PANEL — controls                                               */}
      {/* ------------------------------------------------------------------ */}
      <aside className="flex w-80 shrink-0 flex-col overflow-y-auto border-r border-slate-200 bg-white dark:border-slate-700 dark:bg-slate-900">
        <div className="border-b border-slate-200 px-4 py-3 dark:border-slate-700">
          <h1 className="text-base font-semibold">Chart Prototype</h1>
          <p className="mt-0.5 text-xs text-slate-500">ECharts · NYISO timeseries</p>
        </div>

        <div className="flex flex-1 flex-col gap-4 px-4 py-4">
          {/* Chart type */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Chart Type
            </label>
            <div className="flex flex-wrap gap-1">
              {CHART_TYPES.map((ct) => (
                <button
                  key={ct.value}
                  onClick={() => setWsField("chartType", ct.value)}
                  className={`rounded px-2.5 py-1 text-xs font-medium transition-colors ${
                    ws.chartType === ct.value
                      ? "bg-blue-600 text-white"
                      : "bg-slate-100 text-slate-700 hover:bg-slate-200 dark:bg-slate-700 dark:text-slate-300"
                  }`}
                >
                  {ct.label}
                </button>
              ))}
            </div>
          </div>

          {/* Source System */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Source System
            </label>
            <select
              className="form-input w-full text-sm"
              value={ws.sourceSystem}
              onChange={(e) => {
                setWsField("sourceSystem", e.target.value);
                setWsField("datasetKey", "");
                setWsField("yColumnIds", []);
              }}
            >
              <option value="">— any —</option>
              {sourceSystems.map((s) => (
                <option key={s} value={s}>
                  {s}
                </option>
              ))}
            </select>
          </div>

          {/* Dataset Key */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Dataset
            </label>
            <select
              className="form-input w-full text-sm"
              value={ws.datasetKey}
              onChange={(e) => {
                setWsField("datasetKey", e.target.value);
                setWsField("yColumnIds", []);
              }}
            >
              <option value="">— any —</option>
              {datasetKeys.map((k) => (
                <option key={k} value={k}>
                  {k}
                </option>
              ))}
            </select>
          </div>

          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              X Axis Field
            </label>
            <input
              type="text"
              className="form-input w-full cursor-not-allowed bg-slate-100 text-sm text-slate-500 dark:bg-slate-800 dark:text-slate-400"
              value={ws.xField}
              disabled
              readOnly
            />
          </div>

          {/* Y Fields (multi-select list) */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Y Fields <span className="font-normal text-slate-400">(numeric value series only)</span>
            </label>
            <p className="mb-1 text-[11px] text-slate-500 dark:text-slate-400">
              Timestamp fields are excluded. Dimensions split or filter the returned series instead.
            </p>

            {ws.yColumnIds.length > 0 && (
              <div className="mb-2 rounded border border-slate-200 bg-slate-50 p-2 text-xs dark:border-slate-700 dark:bg-slate-800/70">
                <p className="mb-1 font-medium text-slate-600 dark:text-slate-300">Selected Y series:</p>
                <ul className="space-y-1">
                  {ws.yColumnIds.map((id) => {
                    const col = valueColumns.find((c) => c.column_mapping_id === id);
                    if (!col) return null;
                    return (
                      <li key={id} className="text-slate-600 dark:text-slate-300">
                        {(col.column_label || col.raw_column)}
                        <span className="ml-1 text-slate-400">[{col.semantic_key}]</span>
                        {col.unit_name && <span className="ml-1 text-slate-400">({col.unit_name})</span>}
                      </li>
                    );
                  })}
                </ul>
              </div>
            )}

            <div className="max-h-40 overflow-y-auto rounded border border-slate-200 dark:border-slate-700">
              {filteredColumns.length === 0 ? (
                <p className="px-2 py-2 text-xs text-slate-400">
                  Select a source / dataset above
                </p>
              ) : valueColumns.length === 0 ? (
                <p className="px-2 py-2 text-xs text-slate-400">
                  No plottable value fields were found for this dataset.
                </p>
              ) : (
                valueColumns.map((col) => {
                  const active = ws.yColumnIds.includes(col.column_mapping_id);
                  return (
                    <button
                      key={col.column_mapping_id}
                      onClick={() => toggleYColumn(col.column_mapping_id)}
                      className={`flex w-full items-start gap-1 px-2 py-1.5 text-left text-xs transition-colors ${
                        active
                          ? "bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
                          : "text-slate-700 hover:bg-slate-50 dark:text-slate-300 dark:hover:bg-slate-800"
                      }`}
                    >
                      <span className="mt-px h-3 w-3 shrink-0 rounded-sm border border-current flex items-center justify-center">
                        {active && <span className="block h-1.5 w-1.5 rounded-sm bg-current" />}
                      </span>
                      <span>
                        <span className="font-medium">
                          {col.column_label || col.raw_column}
                        </span>
                        <span className="ml-1 text-slate-400">[{col.semantic_key}]</span>
                        {col.unit_name && <span className="ml-1 text-slate-400">({col.unit_name})</span>}
                        <span className="block text-slate-400">raw: {col.raw_column}</span>
                      </span>
                    </button>
                  );
                })
              )}
            </div>
          </div>

          {/* Date range */}
          <div className="grid grid-cols-2 gap-2">
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                From
              </label>
              <input
                type="date"
                className="form-input w-full text-sm"
                value={ws.dateFrom}
                onChange={(e) => setWsField("dateFrom", e.target.value)}
              />
            </div>
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                To
              </label>
              <input
                type="date"
                className="form-input w-full text-sm"
                value={ws.dateTo}
                onChange={(e) => setWsField("dateTo", e.target.value)}
              />
            </div>
          </div>

          <div className="rounded border border-slate-200 p-3 dark:border-slate-700">
            <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Dimensions</p>
            <label className="mb-2 flex items-start gap-2 text-xs text-slate-600 dark:text-slate-300">
              <input
                type="checkbox"
                className="mt-0.5"
                checked={ws.splitDimensions}
                onChange={(e) => setWsField("splitDimensions", e.target.checked)}
              />
              <span>Split the chart into separate series by dimension value.</span>
            </label>

            {filteredDimTypes.length > 0 && (
              <div className="mb-2">
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                  {ws.splitDimensions ? "Dimension Type to Split" : "Dimension Type Filter"}
                </label>
                <select
                  className="form-input w-full text-sm"
                  value={ws.dimensionType}
                  onChange={(e) => {
                    setWsField("dimensionType", e.target.value);
                    setWsField("dimensionKey", "");
                  }}
                >
                  <option value="">— all —</option>
                  {filteredDimTypes.map((t) => (
                    <option key={t} value={t}>
                      {t}
                    </option>
                  ))}
                </select>
              </div>
            )}

            {filteredDimKeys.length > 0 && (
              <div>
                <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                  Dimension Key Filter
                </label>
                <select
                  className="form-input w-full text-sm"
                  value={ws.dimensionKey}
                  onChange={(e) => setWsField("dimensionKey", e.target.value)}
                >
                  <option value="">— all —</option>
                  {filteredDimKeys.map((k) => (
                    <option key={k} value={k}>
                      {k}
                    </option>
                  ))}
                </select>
              </div>
            )}
          </div>

          {/* Aggregation */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Aggregation
            </label>
            <select
              className="form-input w-full text-sm"
              value={ws.aggregation}
              onChange={(e) => setWsField("aggregation", e.target.value)}
            >
              {AGGREGATIONS.map((a) => (
                <option key={a.value} value={a.value}>
                  {a.label}
                </option>
              ))}
            </select>
          </div>

          {ws.aggregation !== "none" && (
            <div>
              <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
                Agg Function
              </label>
              <select
                className="form-input w-full text-sm"
                value={ws.aggFunc}
                onChange={(e) => setWsField("aggFunc", e.target.value)}
              >
                {AGG_FUNCS.map((f) => (
                  <option key={f.value} value={f.value}>
                    {f.label}
                  </option>
                ))}
              </select>
            </div>
          )}

          {/* Row limit */}
          <div>
            <label className="mb-1 block text-xs font-medium text-slate-600 dark:text-slate-300">
              Row limit
            </label>
            <input
              type="number"
              className="form-input w-full text-sm"
              min={1}
              max={10000}
              value={ws.limit ?? ""}
              onChange={(e) => {
                const nextValue = e.target.value;
                if (nextValue === "") {
                  setWsField("limit", null);
                  return;
                }

                const parsed = Number(nextValue);
                if (!Number.isFinite(parsed)) {
                  return;
                }

                setWsField("limit", Math.max(1, Math.min(10000, parsed)));
              }}
            />
            <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
              Leave blank to use the server default limit of {DEFAULT_LIMIT.toLocaleString()} rows.
            </p>
          </div>

          <div className="rounded border border-slate-200 p-3 dark:border-slate-700">
            <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">Graph Options</p>
            <div className="space-y-2 text-xs text-slate-600 dark:text-slate-300">
              <label className="flex items-start gap-2">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={ws.sumDimensions}
                  onChange={(e) => setWsField("sumDimensions", e.target.checked)}
                />
                <span>Add a total series when dimensions are split.</span>
              </label>
              <label className="flex items-start gap-2">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={ws.stackSeries}
                  onChange={(e) => setWsField("stackSeries", e.target.checked)}
                />
                <span>Stack compatible series.</span>
              </label>
              <label className="flex items-start gap-2">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={ws.smoothLines}
                  onChange={(e) => setWsField("smoothLines", e.target.checked)}
                />
                <span>Smooth line and area charts.</span>
              </label>
              <label className="flex items-start gap-2">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={ws.showMarkers}
                  onChange={(e) => setWsField("showMarkers", e.target.checked)}
                />
                <span>Show point markers.</span>
              </label>
              <label className="flex items-start gap-2">
                <input
                  type="checkbox"
                  className="mt-0.5"
                  checked={ws.stepLines}
                  onChange={(e) => setWsField("stepLines", e.target.checked)}
                />
                <span>Render line and area charts as step lines.</span>
              </label>
            </div>
          </div>

          {/* Run query button */}
          <button
            className="btn-primary w-full"
            onClick={runQuery}
            disabled={loading}
          >
            {loading ? "Loading..." : "Run Query"}
          </button>

          {!loading && !error && !hasQueried && (
            <p className="rounded border border-blue-200 bg-blue-50 px-2 py-1.5 text-xs text-blue-700 dark:border-blue-800 dark:bg-blue-900/30 dark:text-blue-200">
              Hint: pick a dataset and at least one Y field, then run query.
            </p>
          )}

          {showNoDataHint && (
            <p className="rounded border border-amber-200 bg-amber-50 px-2 py-1.5 text-xs text-amber-800 dark:border-amber-800 dark:bg-amber-900/30 dark:text-amber-200">
              No rows matched. Try widening date range, clearing Dimension filters, or selecting a different Dataset/Y Field.
            </p>
          )}

          {error && (
            <p className="rounded bg-red-50 px-2 py-1.5 text-xs text-red-600 dark:bg-red-900/30 dark:text-red-300">
              {error}
            </p>
          )}

          {data.length > 0 && (
            <p className="text-xs text-slate-500">
              {data.length.toLocaleString()} points returned across the selected series.
            </p>
          )}

          {/* Divider */}
          <hr className="border-slate-200 dark:border-slate-700" />

          {/* Save workspace */}
          <div>
            <p className="mb-1.5 text-xs font-semibold text-slate-600 dark:text-slate-300">
              Save Chart
            </p>
            <input
              type="text"
              placeholder="Chart name"
              className="form-input mb-2 w-full text-sm"
              value={saveName}
              onChange={(e) => setSaveName(e.target.value)}
            />
            <textarea
              placeholder="Description (optional)"
              className="form-input mb-2 w-full resize-none text-sm"
              rows={2}
              value={saveDesc}
              onChange={(e) => setSaveDesc(e.target.value)}
            />
            <button
              className="btn-secondary w-full"
              onClick={handleSave}
              disabled={!saveName.trim()}
            >
              {activeChartId ? "Update" : "Save"}
            </button>
            {saveStatus && (
              <p className="mt-1 text-xs text-slate-500">{saveStatus}</p>
            )}
          </div>
        </div>
      </aside>

      {/* ------------------------------------------------------------------ */}
      {/* MIDDLE — chart + saved list                                         */}
      {/* ------------------------------------------------------------------ */}
      <main className="flex flex-1 flex-col overflow-hidden">
        {/* Chart area */}
        <div className="relative flex-1 overflow-hidden p-4">
          {!hasQueried && data.length === 0 && !loading && (
            <div className="absolute inset-0 flex items-center justify-center text-sm text-slate-400">
              Configure filters on the left, then click <strong className="mx-1">Run Query</strong>
            </div>
          )}
          {showNoDataHint && (
            <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
              <div className="rounded border border-amber-200 bg-amber-50/95 px-4 py-3 text-sm text-amber-900 shadow-sm dark:border-amber-800 dark:bg-amber-900/60 dark:text-amber-100">
                No data points returned for the current filter set.
              </div>
            </div>
          )}
          <div ref={chartRef} className="h-full w-full" />
        </div>

        {/* Saved charts shelf */}
        {savedCharts.length > 0 && (
          <div className="border-t border-slate-200 bg-white px-4 py-3 dark:border-slate-700 dark:bg-slate-900">
            <p className="mb-2 text-xs font-semibold text-slate-500 uppercase tracking-wide">
              Saved Charts
            </p>
            <div className="flex gap-2 overflow-x-auto pb-1">
              {savedCharts.map((chart) => (
                <div
                  key={chart.chart_definition_id}
                  className={`flex shrink-0 items-center gap-1.5 rounded border px-3 py-1.5 text-xs transition-colors cursor-pointer ${
                    activeChartId === chart.chart_definition_id
                      ? "border-blue-400 bg-blue-50 text-blue-700 dark:bg-blue-900/30 dark:text-blue-300"
                      : "border-slate-200 bg-slate-50 text-slate-600 hover:border-slate-300 dark:border-slate-700 dark:bg-slate-800 dark:text-slate-300"
                  }`}
                  onClick={() => handleLoad(chart)}
                >
                  <span className="max-w-30 truncate font-medium">{chart.name}</span>
                  <span className="text-slate-400">{chart.chart_type}</span>
                  <button
                    className="ml-1 text-slate-400 hover:text-red-500"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDelete(chart.chart_definition_id);
                    }}
                    title="Delete"
                  >
                    ×
                  </button>
                </div>
              ))}
            </div>
          </div>
        )}
      </main>
    </div>
  );
}
