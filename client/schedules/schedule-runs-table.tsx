/**
 * Compact table showing the run history for a single NYISO schedule.
 * Rows are colour-coded: COMPLETED rows receive a success style; FAILED rows receive a danger style.
 * Accepts pre-fetched `runs` data and a `parseTimestamp` formatter from the parent component.
 *
 * @param runsLoading    - When true and no `runs` are available yet, renders a loading spinner.
 * @param runs           - Array of past run records; when empty renders an empty state.
 * @param parseTimestamp - Formats a nullable ISO timestamp string for display in cells.
 */
import EmptyState from "@templates/empty-state";
import LoadingState from "@templates/loading-state";
import type { NyisoScheduleRun } from "./schedule-api";

type ScheduleRunsTableProps = {
  runsLoading: boolean;
  runs?: NyisoScheduleRun[];
  parseTimestamp: (value: string | null) => string;
};

/** Maps a run's state value to the appropriate table row CSS class string. */
function getRunRowClassName(stateValue: NyisoScheduleRun["state_value"]) {
  const classes = ["data-table__row", "data-table__row--compact"];

  if (stateValue === "FAILED") {
    classes.push("data-table__row--danger");
  }

  if (stateValue === "COMPLETED") {
    classes.push("data-table__row--success");
  }

  return classes.join(" ");
}

export default function ScheduleRunsTable({
  runsLoading,
  runs,
  parseTimestamp,
}: ScheduleRunsTableProps) {
  if (runsLoading && !runs) {
    return <LoadingState label="Loading runs..." />;
  }

  if (!runs || runs.length === 0) {
    return <EmptyState label="No past runs recorded." />;
  }

  return (
    <div className="data-table-wrap">
      <table className="data-table data-table--compact">
        <thead>
          <tr className="data-table__head-row data-table__head-row--strong">
            <th className="data-table__cell data-table__cell--compact">State</th>
            <th className="data-table__cell data-table__cell--compact">Triggered By</th>
            <th className="data-table__cell data-table__cell--compact">Started</th>
            <th className="data-table__cell data-table__cell--compact">Finished</th>
            <th className="data-table__cell data-table__cell--compact">Targeted</th>
            <th className="data-table__cell data-table__cell--compact">Downloaded</th>
            <th className="data-table__cell data-table__cell--compact">Completed</th>
            <th className="data-table__cell data-table__cell--compact">Failed</th>
            <th className="data-table__cell data-table__cell--compact">Message</th>
          </tr>
        </thead>
        <tbody>
          {runs.map((run) => (
            <tr key={run.schedule_run_id} className={getRunRowClassName(run.state_value)}>
              <td className="data-table__cell data-table__cell--compact font-medium">{run.state_value}</td>
              <td className="data-table__cell data-table__cell--compact">{run.triggered_by}</td>
              <td className="data-table__cell data-table__cell--compact">{parseTimestamp(run.started_at)}</td>
              <td className="data-table__cell data-table__cell--compact">{parseTimestamp(run.finished_at)}</td>
              <td className="data-table__cell data-table__cell--compact data-table__cell--numeric">{run.records_targeted}</td>
              <td className="data-table__cell data-table__cell--compact data-table__cell--numeric">{run.files_downloaded}</td>
              <td className="data-table__cell data-table__cell--compact data-table__cell--numeric">{run.completed_count}</td>
              <td className="data-table__cell data-table__cell--compact data-table__cell--numeric">{run.failed_count}</td>
              <td className="data-table__cell data-table__cell--compact data-table__cell--muted data-table__message-cell" title={run.message}>
                {run.message || "-"}
              </td>
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  );
}
