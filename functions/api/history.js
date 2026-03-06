import { jsonError } from './_helpers.js';

export async function onRequestGet(context) {
    const db = context.env.DB;
    const url = new URL(context.request.url);
    const days = Number(url.searchParams.get('days') || 7);
    const withSources = url.searchParams.get('withSources') !== 'false';
    const cutoff = new Date();
    cutoff.setUTCDate(cutoff.getUTCDate() - days);
    const cutoffDate = cutoff.toISOString().slice(0, 10);

    try {
        const snapshots = await db.prepare(
            `SELECT snapshot_date, snapshot_json, created_at
             FROM market_snapshots
             WHERE snapshot_date >= ?
             ORDER BY snapshot_date DESC`
        ).bind(cutoffDate).all();

        const data = {
            success: true,
            days,
            snapshots: (snapshots.results || []).map((row) => ({
                snapshotDate: row.snapshot_date,
                createdAt: row.created_at,
                data: JSON.parse(row.snapshot_json),
            })),
        };

        if (withSources) {
            const sources = await db.prepare(
                `SELECT source_name, module_name, file_name, as_of_date, created_at
                 FROM source_files
                 WHERE as_of_date >= ? OR created_at >= datetime('now', ?)
                 ORDER BY created_at DESC
                 LIMIT 500`
            ).bind(cutoffDate, `-${days} day`).all();
            data.sources = sources.results || [];
        }

        return Response.json(data);
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
