/**
 * API client for NYISO report metadata and content endpoints.
 *
 * Endpoint map:
 *  - report_list/         — GET paginated list of all nyiso_report rows
 *  - report_rows/         — GET multiple report rows by ID list (for poll-patch updates)
 *  - report_row/          — GET a single report row by report_id
 *  - report_row/content/  — GET the pre-parsed file content payload for a report
 *  - report_row/refresh/  — POST to re-queue a single report row for metadata refresh
 *  - report_refresh/start/  — POST to start a background metadata refresh for all reports
 *  - report_refresh/status/ — GET the current refresh task status (used for polling)
 */
import { parseJsonResponse } from "@/utils/api-json";
import { fetchWithRetry } from "@/utils/api-fetch";

export const NYISO_REPORT_LIST_ENDPOINT = "/api/energy_hub/report_list/";
export const NYISO_REPORT_ROWS_ENDPOINT = "/api/energy_hub/report_rows/";
export const NYISO_REPORT_ROW_ENDPOINT = "/api/energy_hub/report_row/";
export const NYISO_REPORT_ROW_CONTENT_ENDPOINT = "/api/energy_hub/report_row/content/";
export const NYISO_REPORT_ROW_REFRESH_ENDPOINT = "/api/energy_hub/report_row/refresh/";
export const NYISO_REPORT_REFRESH_START_ENDPOINT = "/api/energy_hub/report_refresh/start/";
export const NYISO_REPORT_REFRESH_STATUS_ENDPOINT = "/api/energy_hub/report_refresh/status/";

type NyisoTaskStartPayload = {
	queued_report_ids: number[];
	active_report_ids: number[];
	queued_count: number;
	active_count: number;
	cursor: string;
};

type NyisoTaskPollPayload = {
	active_report_ids: number[];
	finished_report_ids: number[];
	active_count: number;
	finished_count: number;
	cursor: string;
};

type NyisoReportRefreshActionPayload = {
	report_id: number;
	task_id: string;
	queued: boolean;
	message: string;
};

/** Internal helper that wraps fetchWithRetry + parseJsonResponse for GET requests. */
async function fetchJson<TPayload>(url: string): Promise<TPayload> {
	const response = await fetchWithRetry(url);
	return parseJsonResponse<TPayload>(response);
}

/**
 * Queues a background metadata refresh for all reports.
 * @param forceReinsert - When true, re-inserts records even if they already exist.
 * @returns Cursor and queue/active counts for subsequent polling.
 */
export async function startNyisoReportRefresh(forceReinsert = false): Promise<NyisoTaskStartPayload> {
	const url = new URL(NYISO_REPORT_REFRESH_START_ENDPOINT, window.location.origin);
	if (forceReinsert) {
		url.searchParams.set("force_reinsert", "true");
	}
	return fetchJson<NyisoTaskStartPayload>(url.toString());
}

/**
 * Polls whether any reports finished refreshing since the last call.
 * @param cursor - Opaque cursor string from the previous poll or start response;
 *                 pass undefined for the first poll.
 * @returns Active/finished report IDs and an updated cursor for the next poll.
 */
export async function pollNyisoReportRefreshStatus(cursor?: string): Promise<NyisoTaskPollPayload> {
	const url = new URL(NYISO_REPORT_REFRESH_STATUS_ENDPOINT, window.location.origin);
	if (cursor) {
		url.searchParams.set("since", cursor);
	}
	return fetchJson<NyisoTaskPollPayload>(url.toString());
}

/**
 * Fetches multiple report rows by ID to apply as row-level patches to the grid
 * (avoids a full reload after a background refresh completes).
 * Returns an empty array when `ids` is empty or contains no valid integers.
 */
export async function fetchNyisoReportRowsByIds<TRow extends { nyiso_report_id: number }>(
	ids: number[]
): Promise<TRow[]> {
	if (ids.length === 0) {
		return [];
	}

	const uniqueIds = [...new Set(ids)].filter((id) => Number.isInteger(id) && id > 0);
	if (uniqueIds.length === 0) {
		return [];
	}

	const url = new URL(NYISO_REPORT_ROWS_ENDPOINT, window.location.origin);
	url.searchParams.set("ids", uniqueIds.join(","));
	return fetchJson<TRow[]>(url.toString());
}

/** Fetches the full metadata row for a single report by its numeric ID. */
export async function fetchNyisoReportRowById<TRow>(reportId: number): Promise<TRow> {
	const url = new URL(NYISO_REPORT_ROW_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));
	return fetchJson<TRow>(url.toString());
}

/** Fetches the pre-parsed file content payload for a single report. */
export async function fetchNyisoReportRowContent<TPayload>(reportId: number): Promise<TPayload> {
	const url = new URL(NYISO_REPORT_ROW_CONTENT_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));
	return fetchJson<TPayload>(url.toString());
}

/** Re-queues a single report row for a metadata/content refresh. */
export async function refreshNyisoReportRow(reportId: number): Promise<NyisoReportRefreshActionPayload> {
	const url = new URL(NYISO_REPORT_ROW_REFRESH_ENDPOINT, window.location.origin);
	url.searchParams.set("report_id", String(reportId));

	const response = await fetchWithRetry(url.toString(), { method: "POST" });
	return parseJsonResponse<NyisoReportRefreshActionPayload>(response);
}
