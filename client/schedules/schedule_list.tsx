import { Fragment, useEffect, useMemo, useRef, useState } from "react";
import { createPortal } from "react-dom";

import EmptyState from "@templates/empty-state";
import ErrorBanner from "@templates/error-banner";
import FormBuilder from "@templates/form-builder";
import FormBody from "@templates/form-body";
import LoadingState from "@templates/loading-state";
import SectionPanel from "@templates/section-panel";
import SuccessBanner from "@templates/success-banner";
import { useFormEngine } from "@/hooks/use-form-engine";
import { fetchWithRetry } from "@/utils/api-fetch";
import { parseJsonResponse } from "@/utils/api-json";
import { formatNullableTimestamp } from "@/utils/display-format";

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
  type NyisoScheduleRun,
  type NyisoScheduleTestResponse,
} from "./schedule-api";
import {
  createScheduleFormSchema,
  mapScheduleToFormValues,
} from "./schedule-form.schema";
import ScheduleRunsTable from "./schedule-runs-table";

type NyisoReportOption = {
  nyiso_report_id: number;
  code: string;
  name: string;
};

function unwrapListPayload<T>(data: T[] | { items?: T[]; results?: T[] }): T[] {
  if (Array.isArray(data)) {
    return data;
  }
  return data.items ?? data.results ?? [];
}

function getPreviewCallClassName(foundOnPage: boolean | null | undefined) {
  const classes = ["preview-panel__item"];

  if (foundOnPage === true) {
    classes.push("preview-panel__item--success");
  } else if (foundOnPage === false) {
    classes.push("preview-panel__item--danger");
  } else {
    classes.push("preview-panel__item--neutral");
  }

  return classes.join(" ");
}

function getScheduleRowClassName(isSelected: boolean) {
  const classes = ["data-table__row", "data-table__row--interactive"];

  if (isSelected) {
    classes.push("data-table__row--selected");
  }

  return classes.join(" ");
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

  const reportOptions = useMemo(
    () => reports.map((report) => ({
      value: String(report.nyiso_report_id),
      label: `${report.code} - ${report.name}`,
    })),
    [reports]
  );

  const formSchema = useMemo(() => createScheduleFormSchema(reportOptions), [reportOptions]);
  const {
    values: formValues,
    errors: formErrors,
    setFieldValue,
    replaceValues,
    clearErrors,
    reset: resetForm,
    validate: validateForm,
    buildPayload,
  } = useFormEngine(formSchema);

  const fetchReports = async (): Promise<NyisoReportOption[]> => {
    const response = await fetchWithRetry(NYISO_REPORT_LIST_ENDPOINT);
    const payload = await parseJsonResponse<NyisoReportOption[] | { items?: NyisoReportOption[]; results?: NyisoReportOption[] }>(response);
    return unwrapListPayload(payload);
  };

  const refreshData = async () => {
    setLoading(true);
    try {
      const [scheduleRows, reportRows] = await Promise.all([
        listNyisoSchedules(),
        fetchReports(),
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

  const onTestSchedule = async () => {
    if (!validateForm()) {
      setError("Please correct highlighted form fields.");
      return;
    }

    setTesting(true);
    setMessage(null);
    setError(null);
    setTestResult(null);
    clearErrors();

    try {
      const payload = buildPayload();
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
    if (!validateForm()) {
      setError("Please correct highlighted form fields.");
      return;
    }

    setSaving(true);
    setMessage(null);
    setError(null);
    clearErrors();

    try {
      const payload = buildPayload();
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
    replaceValues(mapScheduleToFormValues(schedule));
    setTestResult(null);
    setMessage(null);
    setError(null);
    formRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  };

  const onCancelEdit = () => {
    setEditingScheduleId(null);
    resetForm();
    setTestResult(null);
    setMessage(null);
    setError(null);
    clearErrors();
  };

  const onUpdateSchedule = async () => {
    if (editingScheduleId === null) return;
    if (!validateForm()) {
      setError("Please correct highlighted form fields.");
      return;
    }

    setSaving(true);
    setMessage(null);
    setError(null);
    clearErrors();
    try {
      const payload = buildPayload();
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
      {message && <SuccessBanner message={message} />}
      {error && <ErrorBanner message={error} onRetry={() => void refreshData()} />}

      <SectionPanel title={editingScheduleId !== null ? "Edit Schedule" : "Create Schedule"}>
        <div ref={formRef}>
          <FormBuilder
            schema={formSchema}
            values={formValues}
            errors={formErrors}
            disabled={saving || testing}
            onChange={setFieldValue}
          />

          <div className="action-row">
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
            <div className="preview-panel">
              <h4 className="font-semibold">Dry-run Preview</h4>
              <p className="preview-panel__meta">
                {testResult.message} Mode: {testResult.resolved_mode}. Reports: {testResult.report_count}. Planned calls: {testResult.estimated_call_count}.
              </p>
              <div className="preview-panel__body">
                {testResult.calls.length === 0 ? (
                  <p className="preview-panel__empty">No calls planned.</p>
                ) : (
                  <ul className="preview-panel__list">
                    {testResult.calls.map((call, idx) => (
                      <li
                        key={`${call.task_name}-${call.report_id ?? "none"}-${call.run_date ?? "none"}-${idx}`}
                        className={getPreviewCallClassName(call.found_on_page)}
                      >
                        <div className="font-medium">
                          {call.report_code ?? "ALL"} | date: {call.run_date ?? "-"}{" "}
                          {call.found_on_page === true && (
                            <span className="preview-panel__status--success">(found)</span>
                          )}
                          {call.found_on_page === false && (
                            <span className="preview-panel__status--danger">(not found)</span>
                          )}
                        </div>
                        {call.matched_href && (
                          <div className="preview-panel__link">{call.matched_href}</div>
                        )}
                        {call.resolved_url && (
                          <div className="preview-panel__link preview-panel__link--accent">
                            {call.resolved_url}
                          </div>
                        )}
                        {call.note && <div className="preview-panel__note">{call.note}</div>}
                      </li>
                    ))}
                  </ul>
                )}
              </div>
            </div>
          )}
        </div>
      </SectionPanel>

      <SectionPanel title="Existing Schedules">
        {loading ? (
          <LoadingState label="Loading schedules..." />
        ) : (
          <div className="data-table-wrap">
            <table className="data-table data-table--regular">
              <thead>
                <tr className="data-table__head-row data-table__head-row--muted">
                  <th className="data-table__cell">Name</th>
                  <th className="data-table__cell">Mode</th>
                  <th className="data-table__cell">Report</th>
                  <th className="data-table__cell">Interval</th>
                  <th className="data-table__cell">State</th>
                  <th className="data-table__cell">Next Run</th>
                  <th className="data-table__cell">Last Run</th>
                </tr>
              </thead>
              <tbody>
                {schedules.map((schedule) => {
                  const sid = schedule.nyiso_report_schedule_id;
                  const runsExpanded = expandedRunsId === sid;
                  const runs = runsMap[sid];
                  return (
                    <Fragment key={`row-${sid}`}>
                      <tr
                        className={getScheduleRowClassName(editingScheduleId === sid)}
                        onContextMenu={(e) => openMenu(e, schedule)}
                      >
                        <td className="data-table__cell">{schedule.name}</td>
                        <td className="data-table__cell">{schedule.mode}</td>
                        <td className="data-table__cell">{schedule.report_code ?? "ALL"}</td>
                        <td className="data-table__cell">{schedule.interval_minutes} min</td>
                        <td className="data-table__cell">{schedule.is_active ? schedule.last_state : "PAUSED"}</td>
                        <td className="data-table__cell">{schedule.is_active ? formatNullableTimestamp(schedule.next_run_at) : "-"}</td>
                        <td className="data-table__cell">{formatNullableTimestamp(schedule.last_run_at)}</td>
                      </tr>
                      {runsExpanded && (
                        <tr key={`runs-${sid}`} className="data-table__row">
                          <td colSpan={7} className="data-table__cell data-table__cell--detail">
                            <ScheduleRunsTable
                              runsLoading={runsLoading}
                              runs={runs}
                              parseTimestamp={formatNullableTimestamp}
                            />
                          </td>
                        </tr>
                      )}
                    </Fragment>
                  );
                })}
                {schedules.length === 0 && (
                  <tr>
                    <td className="empty-state-cell" colSpan={7}>
                      <EmptyState label="No schedules defined." />
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        )}
      </SectionPanel>

      {menu && createPortal(
        <div
          className="context-menu"
          style={{ left: menu.x, top: menu.y, zIndex: 2000 }}
          role="menu"
          onPointerDown={(e) => e.stopPropagation()}
        >
          <button
            type="button" role="menuitem"
            className="context-menu__item"
            onClick={() => { void onRunNow(menu.schedule); setMenu(null); }}
          >
            Run Now
          </button>
          <button
            type="button" role="menuitem"
            className="context-menu__item"
            onClick={() => { void onToggleSchedule(menu.schedule); setMenu(null); }}
          >
            {menu.schedule.is_active ? "Pause" : "Resume"}
          </button>
          <button
            type="button" role="menuitem"
            className="context-menu__item"
            onClick={() => { onEditSchedule(menu.schedule); setMenu(null); }}
          >
            Edit
          </button>
          <button
            type="button" role="menuitem"
            className="context-menu__item"
            onClick={() => { void onToggleRuns(menu.schedule); setMenu(null); }}
          >
            {expandedRunsId === menu.schedule.nyiso_report_schedule_id ? "Hide Runs" : "View Runs"}
          </button>
          <div className="my-1 border-t border-slate-100 dark:border-slate-700" />
          <button
            type="button" role="menuitem"
            className="context-menu__item text-red-600 hover:bg-red-50 dark:text-red-400 dark:hover:bg-red-950"
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
