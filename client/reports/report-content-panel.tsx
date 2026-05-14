/**
 * Renders the file content for a single NYISO report in the detail view.
 * Supports three content modes returned by the API:
 *  - `"FILE_MATRIX"`     — date-indexed matrix of file URLs (one column per file type)
 *  - `"SINGULAR_FILES"`  — flat list of individually named file links
 *  - `"INLINE_FEED"`     — chronological message/event feed
 *
 * Renders loading, error, and empty states internally so the parent only needs to
 * pass the result of its content fetch and a retry callback.
 */
import EmptyState from "@templates/empty-state";
import ErrorBanner from "@templates/error-banner";
import LoadingState from "@templates/loading-state";
import SectionPanel from "@templates/section-panel";

export type MatrixRow = {
  date: string;
  last_updated: string;
  links: Record<string, string>;
};

export type SingularRow = {
  label: string;
  url: string;
};

export type InlineFeedRow = {
  message_type: string;
  time: string;
  message: string;
};

export type ReportDetailContentPayload = {
  mode: "FILE_MATRIX" | "SINGULAR_FILES" | "INLINE_FEED";
  file_types: string[];
  rows: MatrixRow[] | SingularRow[] | InlineFeedRow[];
  report_id: number;
};

type ReportContentPanelProps = {
  content: ReportDetailContentPayload | null;
  contentLoading: boolean;
  contentError: string | null;
  onRetry: () => void;
};

const CONTENT_GRID_CONTAINER_CLASS =
  "min-h-0 overflow-auto rounded-md border border-ui-border w-full";
const CONTENT_GRID_HEADER_CELL_CLASS =
  "sticky top-0 z-10 bg-slate-50 px-4 py-2 dark:bg-slate-800";

export default function ReportContentPanel({
  content,
  contentLoading,
  contentError,
  onRetry,
}: ReportContentPanelProps) {
  return (
    <SectionPanel className="mt-2 flex min-h-0 flex-1 flex-col" title="Report Content">

      {contentLoading && <LoadingState label="Loading report content..." />}
      {contentError && <ErrorBanner message={contentError} onRetry={onRetry} />}
      {!contentLoading && !contentError && !content && <EmptyState label="No report content available." />}

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
    </SectionPanel>
  );
}
