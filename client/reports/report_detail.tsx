import { useEffect, useMemo, useState } from "react";
import { useParams } from "react-router-dom";

import FormBody from "@templates/form-body";

import {
  fetchNyisoReportRowContent,
  fetchNyisoReportRowById,
  pollNyisoReportRefreshStatus,
  refreshNyisoReportRow,
} from "./report-api";

type NyisoReportRow = {
  nyiso_report_id: number;
  code: string;
  name: string;
  content_type: string;
  frequency: string[];
  file_type: string[];
  source_page: string;
  latest_report_stamp: string | null;
  earliest_report_stamp: string | null;
  file_name_format: string;
  parse_status: string;
  task_status: string;
  task_updated_at: string | null;
  last_scanned_at: string | null;
  is_deprecated: boolean;
};

type MatrixRow = {
  date: string;
  last_updated: string;
  links: Record<string, string>;
};

type SingularRow = {
  label: string;
  url: string;
};

type InlineFeedRow = {
  message_type: string;
  time: string;
  message: string;
};

type ReportDetailContentPayload = {
  mode: "FILE_MATRIX" | "SINGULAR_FILES" | "INLINE_FEED";
  file_types: string[];
  rows: MatrixRow[] | SingularRow[] | InlineFeedRow[];
  report_id: number;
};

const CONTENT_GRID_CONTAINER_CLASS =
  "min-h-0 overflow-auto rounded-md border border-ui-border w-full";
const CONTENT_GRID_HEADER_CELL_CLASS =
  "sticky top-0 z-10 bg-slate-50 px-4 py-2 dark:bg-slate-800";

const ACTIVE_STATES = new Set(["QUEUED", "RUNNING"]);

export default function NyisoReportDetail() {
  const { reportId } = useParams<{ reportId: string }>();
  const parsedId = Number(reportId);

  const [row, setRow] = useState<NyisoReportRow | null>(null);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [actionMessage, setActionMessage] = useState<string | null>(null);
  const [content, setContent] = useState<ReportDetailContentPayload | null>(null);
  const [contentLoading, setContentLoading] = useState(false);
  const [contentError, setContentError] = useState<string | null>(null);

  const loadContent = async (targetReportId: number) => {
    setContentLoading(true);
    try {
      const payload = await fetchNyisoReportRowContent<ReportDetailContentPayload>(targetReportId);
      setContent(payload);
      setContentError(null);
    } catch (err) {
      setContentError((err as Error).message);
      setContent(null);
    } finally {
      setContentLoading(false);
    }
  };

  const loadRow = async () => {
    if (!Number.isInteger(parsedId) || parsedId <= 0) {
      setError("Invalid report id.");
      setLoading(false);
      return;
    }

    try {
      const payload = await fetchNyisoReportRowById<NyisoReportRow>(parsedId);
      setRow(payload);
      await loadContent(payload.nyiso_report_id);
      setError(null);
    } catch (err) {
      setError((err as Error).message);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    void loadRow();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [parsedId]);

  useEffect(() => {
    if (!row || !ACTIVE_STATES.has(row.task_status)) {
      return;
    }

    let mounted = true;
    const timer = window.setInterval(async () => {
      try {
        const status = await pollNyisoReportRefreshStatus();
        if (!mounted) {
          return;
        }

        const isFinished = status.finished_report_ids.includes(row.nyiso_report_id);
        const isStillActive = status.active_report_ids.includes(row.nyiso_report_id);

        if (isFinished || !isStillActive) {
          await loadRow();
          setActionMessage(null);
        }
      } catch {
        // Keep polling; next interval retries.
      }
    }, 5000);

    return () => {
      mounted = false;
      window.clearInterval(timer);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [row?.nyiso_report_id, row?.task_status]);

  const onRefresh = async () => {
    if (!row) {
      return;
    }

    setRefreshing(true);
    setActionMessage(null);

    try {
      const payload = await refreshNyisoReportRow(row.nyiso_report_id);
      setActionMessage(payload.message);
      await loadRow();
    } catch (err) {
      setActionMessage((err as Error).message);
    } finally {
      setRefreshing(false);
    }
  };

  const detailRows = useMemo(() => {
    if (!row) {
      return [] as Array<{ label: string; value: string }>;
    }

    const renderTimestamp = (value: string | null) => {
      if (!value) {
        return "-";
      }
      const timestamp = new Date(value);
      return Number.isNaN(timestamp.getTime()) ? value : timestamp.toLocaleString();
    };

    return [
      { label: "ID", value: String(row.nyiso_report_id) },
      { label: "Code", value: row.code },
      { label: "Name", value: row.name },
      { label: "Frequency", value: row.frequency.length ? row.frequency.join(", ") : "-" },
      { label: "File Type", value: row.file_type.length ? row.file_type.join(", ") : "-" },
      { label: "Source Page", value: row.source_page || "-" },
      { label: "File Name Format", value: row.file_name_format || "-" },
      { label: "Parse Status", value: row.parse_status },
      { label: "Task Status", value: row.task_status },
      { label: "Task Updated", value: renderTimestamp(row.task_updated_at) },
      { label: "Latest Stamp", value: renderTimestamp(row.latest_report_stamp) },
      { label: "Earliest Stamp", value: renderTimestamp(row.earliest_report_stamp) },
      { label: "Last Scanned", value: renderTimestamp(row.last_scanned_at) },
      { label: "Deprecated", value: row.is_deprecated ? "Yes" : "No" },
    ];
  }, [row]);

  return (
    <FormBody
      title="NYISO Report Detail"
      subtitle="Read-only report row data with optional backend refresh for this report."
      className="flex h-[calc(100dvh)] flex-col overflow-hidden"
      bodyClassName="flex min-h-0 flex-1 flex-col space-y-4"
    >
      <div className="mb-4 flex items-center gap-3">
        <button type="button" className="btn-primary" onClick={onRefresh} disabled={!row || refreshing}>
          {refreshing ? "Refreshing..." : "Refresh Report"}
        </button>
        <a className="btn-secondary" href="/energy_hub/reports">
          Back to Reports
        </a>
      </div>

      {actionMessage && <div className="error-banner mb-4">{actionMessage}</div>}

      {loading && <div className="body-text">Loading report row...</div>}
      {error && <div className="error-banner">{error}</div>}

      {!loading && !error && row && (
        <>
          <div className="overflow-x-auto rounded-md border border-ui-border">
            <table className="w-full table-auto border-collapse text-sm">
              <tbody>
                {detailRows.map((entry) => (
                  <tr key={entry.label} className="border-t border-slate-100 dark:border-slate-800">
                    <th className="w-52 bg-slate-50 px-4 py-2 text-left font-medium text-slate-700 dark:bg-slate-800 dark:text-slate-300">
                      {entry.label}
                    </th>
                    <td className="px-4 py-2 text-slate-700 dark:text-slate-300">
                      {entry.label === "Source Page" && row.source_page ? (
                        <a
                          className="hyperlink"
                          href={`https://mis.nyiso.com/public/${row.source_page}`}
                          target="_blank"
                          rel="noopener noreferrer"
                        >
                          {entry.value}
                        </a>
                      ) : (
                        entry.value
                      )}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </div>

          <div className="mt-6 flex min-h-0 flex-1 flex-col">
            <h3 className="mb-2 text-base font-semibold text-slate-800 dark:text-slate-200">Report Content</h3>

            {contentLoading && <div className="body-text">Loading report content...</div>}
            {contentError && <div className="error-banner mb-3">{contentError}</div>}

            {!contentLoading && !contentError && content && content.mode === "INLINE_FEED" && (
              <div className={CONTENT_GRID_CONTAINER_CLASS}>
                <table className="w-full min-w-full table-auto border-collapse text-sm">
                  <thead>
                    <tr className="bg-slate-50 text-left dark:bg-slate-800">
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Message Type</th>
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Time</th>
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Message</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(content.rows as InlineFeedRow[]).map((item, index) => (
                      <tr key={`${item.time}-${index}`} className="border-t border-slate-100 dark:border-slate-800">
                        <td className="px-4 py-2">{item.message_type || "-"}</td>
                        <td className="px-4 py-2">{item.time || "-"}</td>
                        <td className="px-4 py-2">{item.message || "-"}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {!contentLoading && !contentError && content && content.mode === "SINGULAR_FILES" && (
              <div className={CONTENT_GRID_CONTAINER_CLASS}>
                <table className="w-full min-w-full table-auto border-collapse text-sm">
                  <thead>
                    <tr className="bg-slate-50 text-left dark:bg-slate-800">
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>File</th>
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Download</th>
                    </tr>
                  </thead>
                  <tbody>
                    {(content.rows as SingularRow[]).map((item) => (
                      <tr key={item.url} className="border-t border-slate-100 dark:border-slate-800">
                        <td className="px-4 py-2">{item.label || "-"}</td>
                        <td className="px-4 py-2">
                          <a className="hyperlink" href={item.url} target="_blank" rel="noopener noreferrer">
                            Download
                          </a>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {!contentLoading && !contentError && content && content.mode === "FILE_MATRIX" && (
              <div className={CONTENT_GRID_CONTAINER_CLASS}>
                <table className="w-full min-w-full table-auto border-collapse text-sm">
                  <thead>
                    <tr className="bg-slate-50 text-left dark:bg-slate-800">
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Date</th>
                      <th className={CONTENT_GRID_HEADER_CELL_CLASS}>Last Updated Time</th>
                      {content.file_types.map((fileType) => (
                        <th key={fileType} className={CONTENT_GRID_HEADER_CELL_CLASS}>
                          {fileType}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody>
                    {(content.rows as MatrixRow[]).map((rowItem, index) => (
                      <tr key={`${rowItem.date}-${index}`} className="border-t border-slate-100 dark:border-slate-800">
                        <td className="px-4 py-2">{rowItem.date || "-"}</td>
                        <td className="px-4 py-2">{rowItem.last_updated || "-"}</td>
                        {content.file_types.map((fileType) => {
                          const href = rowItem.links[fileType];
                          return (
                            <td key={fileType} className="px-4 py-2">
                              {href ? (
                                <a className="hyperlink" href={href} target="_blank" rel="noopener noreferrer">
                                  Download
                                </a>
                              ) : (
                                "-"
                              )}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </div>
        </>
      )}
    </FormBody>
  );
}
