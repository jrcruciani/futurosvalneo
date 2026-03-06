import { getModuleStatus, jsonError } from './_helpers.js';

const RUNBOOK = [
    '1) Ingerir NQ realtime desde NinjaTrader (POST /api/ingest/ninja).',
    '2) Ingerir archivos CME del día (M1, M2, M3, M4) via /api/ingest/cme.',
    '3) Ingerir DarkPool con conversionRatio QQQ->NQ (/api/ingest/darkpool).',
    '4) Ingerir QuikVol (archivo + notas) vía /api/ingest/quikvol.',
    '5) Verificar /api/options-data: missingInputs debe quedar vacío.',
    '6) Ejecutar orquestación /api/analysis y validar salida final.',
    '7) Guardar snapshot /api/snapshot para trazabilidad diaria.',
];

const RETENTION_POLICY = {
    ninjatraderRealtimeDays: 30,
    cmeAndDarkpoolDays: 180,
    snapshotsDays: 365,
    notes: 'Ajustable según coste y compliance; aplicar lifecycle en R2 y purga periódica en D1.',
};

export async function onRequestGet(context) {
    const db = context.env.DB;
    try {
        const moduleStatus = await getModuleStatus(db);
        const missing = moduleStatus.filter((m) => m.status !== 'ready').map((m) => m.label);
        const latestSnapshot = await db.prepare(
            `SELECT snapshot_date, created_at
             FROM market_snapshots
             ORDER BY snapshot_date DESC
             LIMIT 1`
        ).first();

        return Response.json({
            success: true,
            generatedAt: new Date().toISOString(),
            runbook: RUNBOOK,
            retentionPolicy: RETENTION_POLICY,
            moduleStatus,
            pendingModules: missing,
            latestSnapshot: latestSnapshot || null,
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
