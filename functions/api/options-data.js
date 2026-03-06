import { buildDashboardData, jsonError } from './_helpers.js';

function getCurrentSessionId() {
    const now = new Date();
    const utcHour = now.getUTCHours();
    const today = now.toISOString().split('T')[0];
    if (utcHour < 7) {
        const y = new Date(now);
        y.setUTCDate(y.getUTCDate() - 1);
        return `${y.toISOString().split('T')[0]}_nyc`;
    }
    if (utcHour < 13) return `${today}_london`;
    return `${today}_nyc`;
}

function getSessionLabel(sessionId) {
    return sessionId.endsWith('_london') ? 'Pre-London (07:00 UTC)' : 'Pre-NYC (13:00 UTC)';
}

async function getCachedData(db, cacheKey, sessionId) {
    const row = await db.prepare(
        `SELECT data_json, session_id
         FROM data_cache
         WHERE cache_key = ?`
    ).bind(cacheKey).first();

    if (!row || row.session_id !== sessionId) return null;
    return JSON.parse(row.data_json);
}

async function setCachedData(db, cacheKey, sessionId, payload) {
    await db.prepare(
        `INSERT INTO data_cache (cache_key, session_id, data_json, created_at)
         VALUES (?, ?, ?, datetime('now'))
         ON CONFLICT(cache_key)
         DO UPDATE SET session_id = excluded.session_id, data_json = excluded.data_json, created_at = datetime('now')`
    ).bind(cacheKey, sessionId, JSON.stringify(payload)).run();
}

export async function onRequestGet(context) {
    const db = context.env.DB;
    try {
        const url = new URL(context.request.url);
        const forceRefresh = url.searchParams.get('force') === 'true';
        const sessionId = getCurrentSessionId();
        const cacheKey = 'neo_options_data';

        if (!forceRefresh) {
            const cached = await getCachedData(db, cacheKey, sessionId);
            if (cached) {
                cached._fromCache = true;
                cached._session = sessionId;
                cached._sessionLabel = getSessionLabel(sessionId);
                return Response.json(cached);
            }
        }

        const payload = await buildDashboardData(db);
        payload._fromCache = false;
        payload._session = sessionId;
        payload._sessionLabel = getSessionLabel(sessionId);

        await setCachedData(db, cacheKey, sessionId, payload);
        return Response.json(payload);
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
