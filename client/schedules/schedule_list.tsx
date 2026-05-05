import { useEffect, useRef, useState } from "react";
import { createPortal } from "react-dom";

import FormBody from "@templates/form-body";
import { FormFieldLabel } from "@templates/form-field-label";
import { fetchWithRetry } from "@/utils/api-fetch";

import { NYISO_REPORT_LIST_ENDPOINT } from "../reports/report-api";
import {
  createNyisoSchedule,
  deleteNyisoSchedule,
  listNyisoSchedules,
  listScheduleRuns,
  runNyisoScheduleNow,
  testNyisoSchedule,
  toggleNyisoSchedule,
  updateNyisoSchedule,
  type NyisoSchedule,
  type NyisoScheduleCreatePayload,
  type NyisoScheduleRun,
  type NyisoScheduleTestResponse,
} from "./schedule-api";

type NyisoReportOption = {
  nyiso_report_id: number;
  code: string;
  name: string;
};

function parseTimestamp(value: string | null): string {
  if (!value) {
    return "-";
  }
  const dt = new Date(value);
  return Number.isNaN(dt.getTime()) ? value : dt.toLocaleString();
}

export default function NyisoScheduleList() {
  const [schedules, setSchedules] = useState<NyisoSchedule[]>([]);
  const [reports, setReports] = useState<NyisoReportOption[]>([]);
  const [loading, setLoading] = useState(false);
  const [saving, setSaving] = useState(false);
  const [testing, setTesting] = useState(false);
  const [message, setMessage] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [testResult, setTestResult] = useState<NyisoScheduleTestResponse | null>(null);

  // Edit mode: non-null when editing an existing schedule
  const [editingScheduleId, setEditingScheduleId] = useState<number | null>(null);
  // Run history: expanded schedule id + cached runs
  const [expandedRunsId, setExpandedRunsId] = useState<number | null>(null);
  const [runsMap, setRunsMap] = useState<Record<number, NyisoScheduleRun[]>>({});
  const [runsLoading, setRunsLoading] = useState(false);
  const formRef = useRef<HTMLDivElement>(null);
  // Context menu
  const [menu, setMenu] = useState<{ x: number; y: number; schedule: NyisoSchedule } | null>(null);

  const [name, setName] = useState("Daily Metadata Refresh");
  const [mode, setMode] = useState<"METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE">("METADATA_REFRESH");
  const [reportId, setReportId] = useState<string>("");
  const [intervalMinutes, setIntervalMinutes] = useState<number>(1440);
  const [useCache, setUseCache] = useState(true);
  const [runAsync, setRunAsync] = useState(true);
  const [isActive, setIsActive] = useState(true);
  const [startDate, setStartDate] = useState("");
  const [endDate, setEndDate] = useState("");
  const [rollingWindowDays, setRollingWindowDays] = useState<number | "">("");
  const [templateValuesJson, setTemplateValuesJson] = useState('{"fileextension":"csv"}');

  const refreshData = async () => {
    setLoading(true);
    try {
      const [scheduleRows, reportRows] = await Promise.all([
        listNyisoSchedules(),
        fetchWithRetry(NYISO_REPORT_LIST_ENDPOINT).then(async (response) => {
          if (!response.ok) {
            throw new Error(`Reports request failed: ${response.status}`);
          }
          return (await response.json()) as NyisoReportOption[];
        }),
      ]);
      setSchedules(scheduleRows);
      setReports(reportRows);
      setError(null);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void refreshData();
  }, []);

  const buildSchedulePayload = (): NyisoScheduleCreatePayload => {
    let parsedTemplateValues: Record<string, string> = {};
    if (templateValuesJson.trim()) {
      const parsed = JSON.parse(templateValuesJson) as Record<string, unknown>;
      parsedTemplateValues = Object.fromEntries(
        Object.entries(parsed).map(([key, value]) => [String(key), String(value)])
      );
    }

    return {
      name: name.trim(),
      mode,
      report_id: reportId ? Number(reportId) : null,
      is_active: isActive,
      interval_minutes: Math.max(1, Number(intervalMinutes || 1)),
      use_cache: useCache,
      run_async: runAsync,
      start_date: startDate || null,
      end_date: endDate || null,
      rolling_window_days:
        rollingWindowDays === "" ? null : Math.max(1, Number(rollingWindowDays)),
      template_values_json: parsedTemplateValues,
    };
  };

  const onTestSchedule = async () => {
    setTesting(true);
    setMessage(null);
    setError(null);
    setTestResult(null);

    try {
      const payload = buildSchedulePayload();
      const result = await testNyisoSchedule(payload);
      setTestResult(result);
      setMessage("Schedule test completed. No NYISO requests were made.");
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setTesting(false);
    }
  };

  const onCreateSchedule = async () => {
    setSaving(true);
    setMessage(null);
    setError(null);

    try {
      const payload = buildSchedulePayload();
      await createNyisoSchedule(payload);

      setMessage("Schedule created.");
      setTestResult(null);
      await refreshData();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const onToggleSchedule = async (schedule: NyisoSchedule) => {
    setMessage(null);
    setError(null);
    try {
      const response = await toggleNyisoSchedule(schedule.nyiso_report_schedule_id, !schedule.is_active);
      setMessage(response.message);
      await refreshData();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const onRunNow = async (schedule: NyisoSchedule) => {
    setMessage(null);
    setError(null);
    try {
      const response = await runNyisoScheduleNow(
        schedule.nyiso_report_schedule_id,
        schedule.run_async,
        schedule.use_cache
      );
      setMessage(response.message);
      await refreshData();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const onEditSchedule = (schedule: NyisoSchedule) => {
    setEditingScheduleId(schedule.nyiso_report_schedule_id);
    setName(schedule.name);
    setMode(schedule.mode);
    setReportId(schedule.report_id !== null ? String(schedule.report_id) : "");
    setIntervalMinutes(schedule.interval_minutes);
    setUseCache(schedule.use_cache);
    setRunAsync(schedule.run_async);
    setIsActive(schedule.is_active);
    setStartDate(schedule.start_date ?? "");
    setEndDate(schedule.end_date ?? "");
    setRollingWindowDays(schedule.rolling_window_days ?? "");
    setTemplateValuesJson(
      Object.keys(schedule.template_values_json).length > 0
        ? JSON.stringify(schedule.template_values_json, null, 2)
        : '{"fileextension":"csv"}'
    );
    setTestResult(null);
    setMessage(null);
    setError(null);
    formRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const onCancelEdit = () => {
    setEditingScheduleId(null);
    setName("Daily Metadata Refresh");
    setMode("METADATA_REFRESH");
    setReportId("");
    setIntervalMinutes(1440);
    setUseCache(true);
    setRunAsync(true);
    setIsActive(true);
    setStartDate("");
    setEndDate("");
    setRollingWindowDays("");
    setTemplateValuesJson('{"fileextension":"csv"}');
    setTestResult(null);
    setMessage(null);
    setError(null);
  };

  const onUpdateSchedule = async () => {
    if (editingScheduleId === null) return;
    setSaving(true);
    setMessage(null);
    setError(null);
    try {
      const payload = buildSchedulePayload();
      await updateNyisoSchedule(editingScheduleId, payload);
      setMessage("Schedule updated.");
      setTestResult(null);
      onCancelEdit();
      await refreshData();
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setSaving(false);
    }
  };

  const onDeleteSchedule = async (schedule: NyisoSchedule) => {
    if (!confirm(`Delete schedule "${schedule.name}"? This cannot be undone.`)) return;
    setMessage(null);
    setError(null);
    try {
      await deleteNyisoSchedule(schedule.nyiso_report_schedule_id);
      setMessage("Schedule deleted.");
      if (expandedRunsId === schedule.nyiso_report_schedule_id) setExpandedRunsId(null);
      await refreshData();
    } catch (err) {
      setError((err as Error).message);
    }
  };

  const onToggleRuns = async (schedule: NyisoSchedule) => {
    const id = schedule.nyiso_report_schedule_id;
    if (expandedRunsId === id) {
      setExpandedRunsId(null);
      return;
    }
    setExpandedRunsId(id);
    if (!runsMap[id]) {
      setRunsLoading(true);
      try {
        const runs = await listScheduleRuns(id);
        setRunsMap((prev) => ({ ...prev, [id]: runs }));
      } catch (err) {
        setError((err as Error).message);
      } finally {
        setRunsLoading(false);
      }
    }
  };

  const openMenu = (event: React.MouseEvent, schedule: NyisoSchedule) => {
    event.preventDefault();
    event.stopPropagation();
    setMenu({ x: event.clientX, y: event.clientY, schedule });
  };

  useEffect(() => {
    if (!menu) return;
    const close = () => setMenu(null);
    const onKey = (e: KeyboardEvent) => { if (e.key === "Escape") setMenu(null); };
    window.addEventListener("pointerdown", close);
    window.addEventListener("scroll", close, true);
    window.addEventListener("keydown", onKey);
    return () => {
      window.removeEventListener("pointerdown", close);
      window.removeEventListener("scroll", close, true);
      window.removeEventListener("keydown", onKey);
    };
  }, [menu]);

  return (
    <FormBody
      title="NYISO Schedules"
      subtitle="Create and manage recurring metadata refresh and dataset download schedules."
    >
      {message && <div className="rounded border border-emerald-200 bg-emerald-50 px-3 py-2 text-sm">{message}</div>}
      {error && <div className="error-banner">{error}</div>}

      <div className="rounded-md border border-ui-border p-4" ref={formRef}>
        <h3 className="mb-3 text-base font-semibold">
          {editingScheduleId !== null ? "Edit Schedule" : "Create Schedule"}
        </h3>
        <div className="grid grid-cols-1 gap-3 md:grid-cols-2">
          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Name"
              hintInfo="A descriptive label for this schedule."
            />
            <input className="form-input mt-1" value={name} onChange={(e) => setName(e.target.value)} />
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Mode"
              hintInfo="Metadata Refresh updates report listings. File Download Range downloads data files for a date range."
            />
            <select
              className="form-input mt-1"
              value={mode}
              onChange={(e) => setMode(e.target.value as "METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE")}
            >
              <option value="METADATA_REFRESH">Metadata Refresh</option>
              <option value="FILE_DOWNLOAD_RANGE">File Download Range</option>
            </select>
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Report"
              hintInfo="Scope this schedule to a single report, or leave blank to apply to all reports."
            />
            <select className="form-input mt-1" value={reportId} onChange={(e) => setReportId(e.target.value)}>
              <option value="">All Reports</option>
              {reports.map((report) => (
                <option key={report.nyiso_report_id} value={String(report.nyiso_report_id)}>
                  {report.code} - {report.name}
                </option>
              ))}
            </select>
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Interval Minutes"
              hintInfo="How often this schedule runs. 1440 = daily, 60 = hourly."
            />
            <input
              className="form-input mt-1"
              type="number"
              min={1}
              value={intervalMinutes}
              onChange={(e) => setIntervalMinutes(Number(e.target.value))}
            />
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Start Date"
              hintInfo="Optional range start. Leave blank for open start (uses earliest available report date)."
            />
            <input className="form-input mt-1" type="date" value={startDate} onChange={(e) => setStartDate(e.target.value)} />
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="End Date"
              hintInfo="Optional range end. Leave blank for open end (uses today)."
            />
            <input className="form-input mt-1" type="date" value={endDate} onChange={(e) => setEndDate(e.target.value)} />
          </label>

          <label className="text-sm">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Rolling Window Days"
              hintInfo="If set, downloads the last N days on each run, ignoring fixed start/end dates."
            />
            <input
              className="form-input mt-1"
              type="number"
              min={1}
              value={rollingWindowDays}
              onChange={(e) => setRollingWindowDays(e.target.value ? Number(e.target.value) : "")}
            />
          </label>

          <label className="text-sm md:col-span-2">
            <FormFieldLabel
              className="inline-flex items-center"
              label="Template Values JSON"
              hintInfo='Key/value pairs substituted into file URL templates, e.g. {"fileextension": "csv"}.'
            />
            <textarea
              className="form-input mt-1 min-h-24"
              value={templateValuesJson}
              onChange={(e) => setTemplateValuesJson(e.target.value)}
            />
          </label>
        </div>

        <div className="mt-4 flex flex-wrap gap-4 text-sm">
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={isActive} onChange={(e) => setIsActive(e.target.checked)} />
            <FormFieldLabel
              className="inline-flex items-center"
              label="Active"
              hintInfo="When enabled, the schedule will run automatically on the next interval tick."
            />
          </label>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={useCache} onChange={(e) => setUseCache(e.target.checked)} />
            <FormFieldLabel
              className="inline-flex items-center"
              label="Use Cache"
              hintInfo="Skip downloading files that have already been stored in the database."
            />
          </label>
          <label className="inline-flex items-center gap-2">
            <input type="checkbox" checked={runAsync} onChange={(e) => setRunAsync(e.target.checked)} />
            <FormFieldLabel
              className="inline-flex items-center"
              label="Run Async"
              hintInfo="Dispatch the task as a background Celery job instead of running inline."
            />
          </label>
        </div>

        <div className="mt-4 flex gap-3">
          {editingScheduleId !== null ? (
            <>
              <button type="button" className="btn-primary" disabled={saving} onClick={() => void onUpdateSchedule()}>
                {saving ? "Saving..." : "Save Changes"}
              </button>
              <button type="button" className="btn-secondary" onClick={onCancelEdit}>
                Cancel
              </button>
            </>
          ) : (
            <>
              <button type="button" className="btn-primary" disabled={saving} onClick={() => void onCreateSchedule()}>
                {saving ? "Creating..." : "Create Schedule"}
              </button>
              <button type="button" className="btn-secondary" disabled={testing} onClick={() => void onTestSchedule()}>
                {testing ? "Testing..." : "Test Schedule"}
              </button>
            </>
          )}
          <button type="button" className="btn-secondary" disabled={loading} onClick={() => void refreshData()}>
            Reload
          </button>
        </div>

        {testResult && (
          <div className="mt-4 rounded-md border border-slate-200 bg-slate-50 p-3 text-sm dark:border-slate-700 dark:bg-slate-900">
            <h4 className="font-semibold">Dry-run Preview</h4>
            <p className="mt-1 text-xs text-slate-600 dark:text-slate-300">
              {testResult.message} Mode: {testResult.resolved_mode}. Reports: {testResult.report_count}. Planned calls: {testResult.estimated_call_count}.
            </p>
            <div className="mt-2 max-h-56 overflow-auto rounded border border-slate-200 bg-white p-2 text-xs dark:border-slate-700 dark:bg-slate-800">
                  {testResult.calls.length === 0 ? (
                <p className="text-slate-500">No calls planned.</p>
              ) : (
                <ul className="space-y-2">
                  {testResult.calls.map((call, idx) => (
                    <li key={`${call.task_name}-${call.report_id ?? "none"}-${call.run_date ?? "none"}-${idx}`}
                      className={`rounded border p-2 ${
                        call.found_on_page === true
                          ? "border-emerald-200 bg-emerald-50 dark:border-emerald-800 dark:bg-emerald-950"
                          : call.found_on_page === false
                          ? "border-red-200 bg-red-50 dark:border-red-800 dark:bg-red-950"
                          : "border-slate-100 dark:border-slate-700"
                      }`}>
                      <div className="font-medium">
                        {call.report_code ?? "ALL"} | date: {call.run_date ?? "-"}{" "}
                        {call.found_on_page === true && <span className="text-emerald-600 dark:text-emerald-400">(found)</span>}
                        {call.found_on_page === false && <span className="text-red-600 dark:text-red-400">(not found)</span>}
                      </div>
                      {call.matched_href && (
                        <div className="mt-0.5 break-all text-slate-500">{call.matched_href}</div>
                      )}
                      {call.resolved_url && (
                        <div className="mt-0.5 break-all text-blue-600 dark:text-blue-400">{call.resolved_url}</div>
                      )}
                      {call.note && <div className="mt-0.5 text-slate-400">{call.note}</div>}
                    </li>
                  ))}
                </ul>
              )}
            </div>
          </div>
        )}
      </div>

      <div className="rounded-md border border-ui-border p-4">
        <h3 className="mb-3 text-base font-semibold">Existing Schedules</h3>
        {loading ? (
          <p className="body-text">Loading schedules...</p>
        ) : (
          <div className="overflow-x-auto">
            <table className="w-full table-auto border-collapse text-sm">
              <thead>
                <tr className="bg-slate-50 text-left dark:bg-slate-800">
                  <th className="px-3 py-2">Name</th>
                  <th className="px-3 py-2">Mode</th>
                  <th className="px-3 py-2">Report</th>
                  <th className="px-3 py-2">Interval</th>
                  <th className="px-3 py-2">State</th>
                  <th className="px-3 py-2">Next Run</th>
                  <th className="px-3 py-2">Last Run</th>
                </tr>
              </thead>
              <tbody>
                {schedules.map((schedule) => {
                  const sid = schedule.nyiso_report_schedule_id;
                  const runsExpanded = expandedRunsId === sid;
                  const runs = runsMap[sid];
                  return (
                    <>
                      <tr
                        key={sid}
                        className={`cursor-context-menu select-none border-t border-slate-100 dark:border-slate-800 ${editingScheduleId === sid ? "bg-blue-50 dark:bg-blue-950" : "hover:bg-slate-50 dark:hover:bg-slate-800/50"}`}
                        onContextMenu={(e) => openMenu(e, schedule)}
                      >
                        <td className="px-3 py-2">{schedule.name}</td>
                        <td className="px-3 py-2">{schedule.mode}</td>
                        <td className="px-3 py-2">{schedule.report_code ?? "ALL"}</td>
                        <td className="px-3 py-2">{schedule.interval_minutes} min</td>
                        <td className="px-3 py-2">{schedule.is_active ? schedule.last_state : "PAUSED"}</td>
                        <td className="px-3 py-2">{schedule.is_active ? parseTimestamp(schedule.next_run_at) : "-"}</td>
                        <td className="px-3 py-2">{parseTimestamp(schedule.last_run_at)}</td>
                      </tr>
                      {runsExpanded && (
                        <tr key={`runs-${sid}`} className="border-t border-slate-100 dark:border-slate-800">
                          <td colSpan={7} className="bg-slate-50 px-4 py-3 dark:bg-slate-900">
                            {runsLoading && !runs ? (
                              <p className="text-sm text-slate-500">Loading runs...</p>
                            ) : !runs || runs.length === 0 ? (
                              <p className="text-sm text-slate-500">No past runs recorded.</p>
                            ) : (
                              <div className="overflow-x-auto">
                                <table className="w-full table-auto border-collapse text-xs">
                                  <thead>
                                    <tr className="bg-slate-100 text-left dark:bg-slate-800">
                                      <th className="px-2 py-1">State</th>
                                      <th className="px-2 py-1">Triggered By</th>
                                      <th className="px-2 py-1">Started</th>
                                      <th className="px-2 py-1">Finished</th>
                                      <th className="px-2 py-1">Targeted</th>
                                      <th className="px-2 py-1">Downloaded</th>
                                      <th className="px-2 py-1">Completed</th>
                                      <th className="px-2 py-1">Failed</th>
                                      <th className="px-2 py-1">Message</th>
                                    </tr>
                                  </thead>
                                  <tbody>
                                    {runs.map((run) => (
                                      <tr
                                        key={run.schedule_run_id}
                                        className={`border-t border-slate-100 dark:border-slate-700 ${
                                          run.state_value === "FAILED"
                                            ? "bg-red-50 dark:bg-red-950"
                                            : run.state_value === "COMPLETED"
                                            ? "bg-emerald-50 dark:bg-emerald-950"
                                            : ""
                                        }`}
                                      >
                                        <td className="px-2 py-1 font-medium">{run.state_value}</td>
                                        <td className="px-2 py-1">{run.triggered_by}</td>
                                        <td className="px-2 py-1">{parseTimestamp(run.started_at)}</td>
                                        <td className="px-2 py-1">{parseTimestamp(run.finished_at)}</td>
                                        <td className="px-2 py-1 text-center">{run.records_targeted}</td>
                                        <td className="px-2 py-1 text-center">{run.files_downloaded}</td>
                                        <td className="px-2 py-1 text-center">{run.completed_count}</td>
                                        <td className="px-2 py-1 text-center">{run.failed_count}</td>
                                        <td className="max-w-xs truncate px-2 py-1 text-slate-500" title={run.message}>
                                          {run.message || "-"}
                                        </td>
                                      </tr>
                                    ))}
                                  </tbody>
                                </table>
                              </div>
                            )}
                          </td>
                        </tr>
                      )}
                    </>
                  );
                })}
                {schedules.length === 0 && (
                  <tr>
                    <td className="px-3 py-3 text-slate-500" colSpan={7}>
                      No schedules defined.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </div>

      {menu && createPortal(
        <div
          className="fixed min-w-52 rounded-md border border-slate-200 bg-white py-1 shadow-lg dark:border-slate-700 dark:bg-slate-900"
          style={{ left: menu.x, top: menu.y, zIndex: 2000 }}
          role="menu"
          onPointerDown={(e) => e.stopPropagation()}
        >
          <button
            type="button" role="menuitem"
            className="block w-full px-3 py-2 text-left text-sm text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800"
            onClick={() => { void onRunNow(menu.schedule); setMenu(null); }}
          >
            Run Now
          </button>
          <button
            type="button" role="menuitem"
            className="block w-full px-3 py-2 text-left text-sm text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800"
            onClick={() => { void onToggleSchedule(menu.schedule); setMenu(null); }}
          >
            {menu.schedule.is_active ? "Pause" : "Resume"}
          </button>
          <button
            type="button" role="menuitem"
            className="block w-full px-3 py-2 text-left text-sm text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800"
            onClick={() => { onEditSchedule(menu.schedule); setMenu(null); }}
          >
            Edit
          </button>
          <button
            type="button" role="menuitem"
            className="block w-full px-3 py-2 text-left text-sm text-slate-700 hover:bg-slate-100 dark:text-slate-200 dark:hover:bg-slate-800"
            onClick={() => { void onToggleRuns(menu.schedule); setMenu(null); }}
          >
            {expandedRunsId === menu.schedule.nyiso_report_schedule_id ? "Hide Runs" : "View Runs"}
          </button>
          <div className="my-1 border-t border-slate-100 dark:border-slate-700" />
          <button
            type="button" role="menuitem"
            className="block w-full px-3 py-2 text-left text-sm text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950"
            onClick={() => { void onDeleteSchedule(menu.schedule); setMenu(null); }}
          >
            Delete
          </button>
        </div>,
        document.body
      )}
    </FormBody>
  );
}
