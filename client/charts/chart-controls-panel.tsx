/**
 * chart-controls-panel.tsx — Left sidebar control form for chart prototype.
 *
 * Handles chart type selection, data source filters, Y field selection,
 * date ranges, dimensions, aggregation, and graph options.
 * Also includes the Save Chart section.
 */

import type { ColumnMappingOption, DimensionOption } from "./chart-api";
import {
  AGG_FUNCS,
  AGGREGATIONS,
  CHART_TYPES,
  DEFAULT_LIMIT,
} from "./chart-prototype.constants";
import type { WorkspaceState } from "./chart-prototype.types";
import { isValueColumn } from "./chart-prototype.utils";

interface ChartControlsPanelProps {
  ws: WorkspaceState;
  setWsField: <K extends keyof WorkspaceState>(
    k: K,
    v: WorkspaceState[K]
  ) => void;
  columns: ColumnMappingOption[];
  dimensions: DimensionOption[];
  loading: boolean;
  error: string | null;
  hasQueried: boolean;
  data: any[];
  onRunQuery: () => void;
  saveName: string;
  onSaveNameChange: (value: string) => void;
  saveDesc: string;
  onSaveDescChange: (value: string) => void;
  onSave: () => void;
  saveStatus: string | null;
  activeChartId: number | null;
}

export function ChartControlsPanel({
  ws,
  setWsField,
  columns,
  dimensions,
  loading,
  error,
  hasQueried,
  data,
  onRunQuery,
  saveName,
  onSaveNameChange,
  saveDesc,
  onSaveDescChange,
  onSave,
  saveStatus,
  activeChartId,
}: ChartControlsPanelProps) {
  // Derived values
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

  const toggleYColumn = (id: number) => {
    setWsField(
      "yColumnIds",
      ws.yColumnIds.includes(id)
        ? ws.yColumnIds.filter((x) => x !== id)
        : [...ws.yColumnIds, id]
    );
  };

  return (
    <aside className="flex w-80 shrink-0 flex-col overflow-y-auto border-r border-slate-200 bg-white dark:border-slate-700 dark:bg-slate-900">
      <div className="border-b border-slate-200 px-4 py-3 dark:border-slate-700">
        <div className="flex items-center gap-2">
          <h1 className="text-base font-semibold">Chart Prototype</h1>
          <span className="rounded border border-amber-300 bg-amber-100 px-2 py-0.5 text-[10px] font-semibold uppercase tracking-wide text-amber-800 dark:border-amber-700 dark:bg-amber-900/50 dark:text-amber-200">
            Beta
          </span>
        </div>
        <p className="mt-0.5 text-xs text-slate-500">
          ECharts · NYISO timeseries · prototype workflow
        </p>
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

        {/* X Axis Field (locked) */}
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
            Y Fields{" "}
            <span className="font-normal text-slate-400">
              (numeric value series only)
            </span>
          </label>
          <p className="mb-1 text-[11px] text-slate-500 dark:text-slate-400">
            Timestamp fields are excluded. Dimensions split or filter the
            returned series instead.
          </p>

          {ws.yColumnIds.length > 0 && (
            <div className="mb-2 rounded border border-slate-200 bg-slate-50 p-2 text-xs dark:border-slate-700 dark:bg-slate-800/70">
              <p className="mb-1 font-medium text-slate-600 dark:text-slate-300">
                Selected Y series:
              </p>
              <ul className="space-y-1">
                {ws.yColumnIds.map((id) => {
                  const col = valueColumns.find(
                    (c) => c.column_mapping_id === id
                  );
                  if (!col) return null;
                  return (
                    <li
                      key={id}
                      className="text-slate-600 dark:text-slate-300"
                    >
                      {col.column_label || col.raw_column}
                      <span className="ml-1 text-slate-400">
                        [{col.semantic_key}]
                      </span>
                      {col.unit_name && (
                        <span className="ml-1 text-slate-400">
                          ({col.unit_name})
                        </span>
                      )}
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
                const active = ws.yColumnIds.includes(
                  col.column_mapping_id
                );
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
                      {active && (
                        <span className="block h-1.5 w-1.5 rounded-sm bg-current" />
                      )}
                    </span>
                    <span>
                      <span className="font-medium">
                        {col.column_label || col.raw_column}
                      </span>
                      <span className="ml-1 text-slate-400">
                        [{col.semantic_key}]
                      </span>
                      {col.unit_name && (
                        <span className="ml-1 text-slate-400">
                          ({col.unit_name})
                        </span>
                      )}
                      <span className="block text-slate-400">
                        raw: {col.raw_column}
                      </span>
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

        {/* Dimensions */}
        <div className="rounded border border-slate-200 p-3 dark:border-slate-700">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Dimensions
          </p>
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
                {ws.splitDimensions
                  ? "Dimension Type to Split"
                  : "Dimension Type Filter"}
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

              setWsField(
                "limit",
                Math.max(1, Math.min(10000, parsed))
              );
            }}
          />
          <p className="mt-1 text-[11px] text-slate-500 dark:text-slate-400">
            Leave blank to use the server default limit of{" "}
            {DEFAULT_LIMIT.toLocaleString()} rows.
          </p>
        </div>

        {/* Graph Options */}
        <div className="rounded border border-slate-200 p-3 dark:border-slate-700">
          <p className="mb-2 text-xs font-semibold uppercase tracking-wide text-slate-500">
            Graph Options
          </p>
          <div className="space-y-2 text-xs text-slate-600 dark:text-slate-300">
            <label className="flex items-start gap-2">
              <input
                type="checkbox"
                className="mt-0.5"
                checked={ws.sumDimensions}
                onChange={(e) =>
                  setWsField("sumDimensions", e.target.checked)
                }
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
          onClick={onRunQuery}
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
            No rows matched. Try widening date range, clearing Dimension
            filters, or selecting a different Dataset/Y Field.
          </p>
        )}

        {error && (
          <p className="rounded bg-red-50 px-2 py-1.5 text-xs text-red-600 dark:bg-red-900/30 dark:text-red-300">
            {error}
          </p>
        )}

        {data.length > 0 && (
          <p className="text-xs text-slate-500">
            {data.length.toLocaleString()} points returned across the selected
            series.
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
            onChange={(e) => onSaveNameChange(e.target.value)}
          />
          <textarea
            placeholder="Description (optional)"
            className="form-input mb-2 w-full resize-none text-sm"
            rows={2}
            value={saveDesc}
            onChange={(e) => onSaveDescChange(e.target.value)}
          />
          <button
            className="btn-secondary w-full"
            onClick={onSave}
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
  );
}
