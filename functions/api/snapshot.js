import { buildDashboardData, jsonError } from './_helpers.js';

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        const snapshot = await buildDashboardData(db);
        const snapshotDate = new Date().toISOString().slice(0, 10);

        await db.prepare(
            `INSERT INTO market_snapshots (snapshot_date, snapshot_json, created_at)
             VALUES (?, ?, datetime('now'))
             ON CONFLICT(snapshot_date)
             DO UPDATE SET snapshot_json = excluded.snapshot_json, created_at = datetime('now')`
        ).bind(snapshotDate, JSON.stringify(snapshot)).run();

        return Response.json({
            success: true,
            snapshotDate,
            strikes: snapshot.oiDistribution?.strikes?.length || 0,
            mostActive: snapshot.mostActiveStrikes?.length || 0,
            missingInputs: snapshot.missingInputs || [],
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
