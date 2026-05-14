/**
 * Chart Prototype Page — interactive chart builder backed by ECharts.
 *
 * Left panel  : data source + filter controls, save/load workspace
 * Right panel : live ECharts render
 *
 * Data comes from core timeseries_point via /api/core/charts/timeseries/.
 * Chart configs are persisted to core_chart_definition via /api/core/charts/definitions/.
 */

import { useEffect, useRef, useState } from "react";
import * as echarts from "echarts";

import type {
  ChartConfig,
  ChartDefinition,
  ChartType,
  ColumnMappingOption,
  DimensionOption,
} from "./chart-api";
import {
  deleteChartDefinition,
  listChartDefinitions,
  listColumnMappings,
  listDimensions,
  saveChartDefinition,
  updateChartDefinition,
} from "./chart-api";
import { DEFAULT_WORKSPACE } from "./chart-prototype.constants";
import type { WorkspaceState } from "./chart-prototype.types";
import { buildEChartsOption } from "./chart-prototype.utils";
import { useChartWorkspace } from "./use-chart-workspace";
import { useChartQuery } from "./use-chart-query";
import { ChartControlsPanel } from "./chart-controls-panel";
import { SavedChartsShelf } from "./saved-charts-shelf";

export default function ChartPrototypePage() {
  // -- workspace and query hooks
  const { ws, setWsField, setWs } = useChartWorkspace();

  // -- data
  const [columns, setColumns] = useState<ColumnMappingOption[]>([]);
  const [dimensions, setDimensions] = useState<DimensionOption[]>([]);
  const { data, loading, error, hasQueried, runQuery } = useChartQuery({
    columns,
  });

  // -- save state
  const [saveName, setSaveName] = useState("");
  const [saveDesc, setSaveDesc] = useState("");
  const [activeChartId, setActiveChartId] = useState<number | null>(null);
  const [saveStatus, setSaveStatus] = useState<string | null>(null);

  // -- saved charts
  const [savedCharts, setSavedCharts] = useState<ChartDefinition[]>([]);

  // -- ECharts DOM ref
  const chartRef = useRef<HTMLDivElement>(null);
  const chartInstance = useRef<echarts.ECharts | null>(null);

  // Bootstrap: load column mappings (for the y-field picker) and saved chart definitions
  // on mount. Both calls are fire-and-forget — failures are silently swallowed because
  // the chart can still function without pre-loaded values.
  useEffect(() => {
    listColumnMappings().then(setColumns).catch(() => {});
    listChartDefinitions().then(setSavedCharts).catch(() => {});
  }, []);

  // Reload dimension options whenever the source system or dataset key changes so that
  // the dimension filter shows only options relevant to the current data scope.
  useEffect(() => {
    listDimensions(ws.sourceSystem || undefined, ws.datasetKey || undefined)
      .then(setDimensions)
      .catch(() => {});
  }, [ws.sourceSystem, ws.datasetKey]);

  // Initialise the ECharts instance once on mount, bound to the `chartRef` DOM node.
  // Attaches a window resize listener so the chart reflows when the container changes size.
  // Cleanup disposes the ECharts instance and removes the resize listener to prevent
  // memory leaks on unmount.
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

  // Re-render the chart whenever the query data, workspace config, or column list changes.
  // Clears the chart when there is no data so stale series are not left on screen.
  useEffect(() => {
    if (!chartInstance.current) return;
    if (data.length === 0) {
      chartInstance.current.clear();
      return;
    }
    const option = buildEChartsOption(data, ws, columns);
    chartInstance.current.setOption(option, true);
  }, [data, ws, columns]);

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
          prev.map((c) =>
            c.chart_definition_id === activeChartId ? saved : c
          )
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
      chartType: chart.chart_type as ChartType,
    });
    setSaveName(chart.name);
    setSaveDesc(chart.description);
    setActiveChartId(chart.chart_definition_id);
  };

  // -- Delete a saved workspace
  const handleDelete = async (id: number) => {
    try {
      await deleteChartDefinition(id);
      setSavedCharts((prev) =>
        prev.filter((c) => c.chart_definition_id !== id)
      );
      if (activeChartId === id) {
        setActiveChartId(null);
        setSaveName("");
        setSaveDesc("");
      }
    } catch {
      // silent – could surface error toast
    }
  };

  return (
    <div className="flex h-screen overflow-hidden bg-slate-50 dark:bg-slate-900">
      {/* LEFT PANEL — controls */}
      <ChartControlsPanel
        ws={ws}
        setWsField={setWsField}
        columns={columns}
        dimensions={dimensions}
        loading={loading}
        error={error}
        hasQueried={hasQueried}
        data={data}
        onRunQuery={() => runQuery(ws)}
        saveName={saveName}
        onSaveNameChange={setSaveName}
        saveDesc={saveDesc}
        onSaveDescChange={setSaveDesc}
        onSave={handleSave}
        saveStatus={saveStatus}
        activeChartId={activeChartId}
      />

      {/* MIDDLE — chart + saved list */}
      <main className="flex flex-1 flex-col overflow-hidden">
        {/* Chart area */}
        <div className="relative flex-1 overflow-hidden p-4">
          {!hasQueried && data.length === 0 && !loading && (
            <div className="absolute inset-0 flex items-center justify-center text-sm text-slate-400">
              Configure filters on the left, then click{" "}
              <strong className="mx-1">Run Query</strong>
            </div>
          )}
          {hasQueried &&
            !loading &&
            !error &&
            data.length === 0 && (
              <div className="pointer-events-none absolute inset-0 flex items-center justify-center">
                <div className="rounded border border-amber-200 bg-amber-50/95 px-4 py-3 text-sm text-amber-900 shadow-sm dark:border-amber-800 dark:bg-amber-900/60 dark:text-amber-100">
                  No data points returned for the current filter set.
                </div>
              </div>
            )}
          <div ref={chartRef} className="h-full w-full" />
        </div>

        {/* Saved charts shelf */}
        <SavedChartsShelf
          savedCharts={savedCharts}
          activeChartId={activeChartId}
          onLoad={handleLoad}
          onDelete={handleDelete}
        />
      </main>
    </div>
  );
}

