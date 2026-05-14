/**
 * API client for NYISO report schedule management.
 *
 * Endpoint map:
 *  - list/     — GET all schedules
 *  - create/   — POST new schedule
 *  - update/   — PATCH existing schedule (schedule_id query param)
 *  - delete/   — DELETE schedule (schedule_id query param)
 *  - test/     — POST dry-run to preview what a schedule would fetch
 *  - toggle/   — POST to activate/deactivate without a full update
 *  - run/      — POST to trigger an immediate run outside the normal interval
 *  - runs/     — GET past run records for a schedule
 */
import { fetchWithRetry } from "@/utils/api-fetch";
import { parseJsonResponse } from "@/utils/api-json";

export const NYISO_SCHEDULE_LIST_ENDPOINT = "/api/energy_hub/report_schedule/list/";
export const NYISO_SCHEDULE_CREATE_ENDPOINT = "/api/energy_hub/report_schedule/create/";
export const NYISO_SCHEDULE_UPDATE_ENDPOINT = "/api/energy_hub/report_schedule/update/";
export const NYISO_SCHEDULE_DELETE_ENDPOINT = "/api/energy_hub/report_schedule/delete/";
export const NYISO_SCHEDULE_TEST_ENDPOINT = "/api/energy_hub/report_schedule/test/";
export const NYISO_SCHEDULE_TOGGLE_ENDPOINT = "/api/energy_hub/report_schedule/toggle/";
export const NYISO_SCHEDULE_RUN_ENDPOINT = "/api/energy_hub/report_schedule/run/";
export const NYISO_SCHEDULE_RUNS_ENDPOINT = "/api/energy_hub/report_schedule/runs/";

/** Full shape of a NYISO report schedule record as returned by the list/update endpoints. */
export type NyisoSchedule = {
  nyiso_report_schedule_id: number;
  name: string;
  mode: "METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE";
  report_id: number | null;
  report_code: string | null;
  report_name: string | null;
  is_active: boolean;
  interval_minutes: number;
  use_cache: boolean;
  run_async: boolean;
  start_date: string | null;
  end_date: string | null;
  rolling_window_days: number | null;
  template_values_json: Record<string, string>;
  next_run_at: string | null;
  last_run_at: string | null;
  last_state: string;
  last_message: string;
};

/** Request body shape used for both create and update schedule endpoints. */
export type NyisoScheduleCreatePayload = {
  name: string;
  mode: "METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE";
  report_id: number | null;
  is_active: boolean;
  interval_minutes: number;
  use_cache: boolean;
  run_async: boolean;
  start_date: string | null;
  end_date: string | null;
  rolling_window_days: number | null;
  template_values_json: Record<string, string>;
};

/** Result of a single resolved schedule call in a test dry-run. */
export type NyisoScheduleTestCall = {
  task_name: string;
  report_id: number | null;
  report_code: string | null;
  run_date: string | null;
  force_refresh: boolean | null;
  run_async: boolean | null;
  matched_href: string | null;
  resolved_url: string | null;
  found_on_page: boolean | null;
  note: string;
};

/** Overall response from the schedule test (dry-run) endpoint. */
export type NyisoScheduleTestResponse = {
  valid: boolean;
  message: string;
  resolved_mode: "METADATA_REFRESH" | "FILE_DOWNLOAD_RANGE";
  report_count: number;
  estimated_call_count: number;
  calls: NyisoScheduleTestCall[];
};

type NyisoScheduleActionResponse = {
  schedule_id: number;
  ok: boolean;
  message: string;
  task_id: string | null;
};

/** Fetches all NYISO report schedules. */
export async function listNyisoSchedules(): Promise<NyisoSchedule[]> {
  const response = await fetchWithRetry(NYISO_SCHEDULE_LIST_ENDPOINT);
  return parseJsonResponse<NyisoSchedule[]>(response);
}

/** Creates a new NYISO report schedule. Returns the created record. */
export async function createNyisoSchedule(payload: NyisoScheduleCreatePayload): Promise<NyisoSchedule> {
  const response = await fetchWithRetry(NYISO_SCHEDULE_CREATE_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<NyisoSchedule>(response);
}

/**
 * Dry-run test for a schedule payload — resolves which URLs/tasks would be queued
 * without actually triggering downloads.
 */
export async function testNyisoSchedule(payload: NyisoScheduleCreatePayload): Promise<NyisoScheduleTestResponse> {
  const response = await fetchWithRetry(NYISO_SCHEDULE_TEST_ENDPOINT, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<NyisoScheduleTestResponse>(response);
}

/** Toggles a schedule's `is_active` flag without a full update. */
export async function toggleNyisoSchedule(scheduleId: number, isActive: boolean): Promise<NyisoScheduleActionResponse> {
  const url = new URL(NYISO_SCHEDULE_TOGGLE_ENDPOINT, window.location.origin);
  url.searchParams.set("schedule_id", String(scheduleId));
  url.searchParams.set("is_active", String(isActive));

  const response = await fetchWithRetry(url.toString(), { method: "POST" });
  return parseJsonResponse<NyisoScheduleActionResponse>(response);
}

/** Record of a single schedule execution run. */
export type NyisoScheduleRun = {
  schedule_run_id: number;
  state_value: string;
  triggered_by: string;
  celery_task_id: string;
  started_at: string | null;
  finished_at: string | null;
  records_targeted: number;
  files_downloaded: number;
  completed_count: number;
  failed_count: number;
  message: string;
  created_at: string | null;
};

/** Patches an existing schedule by ID. Returns the updated record. */
export async function updateNyisoSchedule(
  scheduleId: number,
  payload: NyisoScheduleCreatePayload
): Promise<NyisoSchedule> {
  const url = new URL(NYISO_SCHEDULE_UPDATE_ENDPOINT, window.location.origin);
  url.searchParams.set("schedule_id", String(scheduleId));
  const response = await fetchWithRetry(url.toString(), {
    method: "PATCH",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(payload),
  });
  return parseJsonResponse<NyisoSchedule>(response);
}

/** Deletes a schedule by ID. Returns void on success (API sends 204). */
export async function deleteNyisoSchedule(scheduleId: number): Promise<void> {
  const url = new URL(NYISO_SCHEDULE_DELETE_ENDPOINT, window.location.origin);
  url.searchParams.set("schedule_id", String(scheduleId));
  const response = await fetchWithRetry(url.toString(), { method: "DELETE" });
  await parseJsonResponse<void>(response);
}

/** Returns the run history for a specific schedule. */
export async function listScheduleRuns(scheduleId: number): Promise<NyisoScheduleRun[]> {
  const url = new URL(NYISO_SCHEDULE_RUNS_ENDPOINT, window.location.origin);
  url.searchParams.set("schedule_id", String(scheduleId));
  const response = await fetchWithRetry(url.toString());
  return parseJsonResponse<NyisoScheduleRun[]>(response);
}

/** Triggers an immediate out-of-band run for the given schedule. */
export async function runNyisoScheduleNow(
  scheduleId: number,
  asyncMode: boolean,
  useCache: boolean
): Promise<NyisoScheduleActionResponse> {
  const url = new URL(NYISO_SCHEDULE_RUN_ENDPOINT, window.location.origin);
  url.searchParams.set("schedule_id", String(scheduleId));
  url.searchParams.set("async_mode", String(asyncMode));
  url.searchParams.set("use_cache", String(useCache));

  const response = await fetchWithRetry(url.toString(), { method: "POST" });
  return parseJsonResponse<NyisoScheduleActionResponse>(response);
}
