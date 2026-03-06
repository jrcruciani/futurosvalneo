import {
    asNumber,
    createSourceFileRecord,
    jsonError,
    normalizeDate,
    storeRawSource,
    upsertVolatilitySignals,
} from '../_helpers.js';

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        const body = await context.request.json();
        const fileName = String(body.fileName || `quikvol-${Date.now()}.json`);
        const asOfDate = normalizeDate(body.asOfDate, new Date().toISOString().slice(0, 10));
        const content = body.content == null ? JSON.stringify(body.signals || []) : String(body.content);
        const mimeType = body.mimeType || 'application/json';

        const stored = await storeRawSource(context.env, {
            category: 'quikvol/images',
            fileName,
            content,
            mimeType,
        });

        const source = await createSourceFileRecord(db, {
            sourceName: 'quikvol',
            moduleName: 'quikvol',
            fileName,
            filePath: stored.key,
            mimeType,
            fileSize: stored.size,
            checksum: stored.checksum,
            asOfDate,
            metadata: { channel: 'api/ingest/quikvol' },
        });

        const incomingSignals = Array.isArray(body.signals) ? body.signals : [];
        const signals = incomingSignals.length
            ? incomingSignals
                .map((s) => ({
                    asOfDate,
                    signalType: String(s.signalType || 'quikvol_signal'),
                    level: asNumber(s.level),
                    value: asNumber(s.value) ?? 0,
                    notes: String(s.notes || ''),
                }))
                .filter((s) => s.signalType)
            : [{
                asOfDate,
                signalType: 'quikvol_uploaded',
                level: -1,
                value: 1,
                notes: 'Se cargó archivo QuikVol sin parseo estructurado.',
            }];

        const inserted = await upsertVolatilitySignals(db, signals, source.id);

        return Response.json({
            success: true,
            sourceFileId: source.id,
            signalsInserted: inserted,
            storedAt: stored.key,
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}

export async function onRequestGet(context) {
    const db = context.env.DB;
    try {
        const rows = await db.prepare(
            `SELECT as_of_date, signal_type, level, value, notes, created_at
             FROM volatility_signals
             WHERE signal_type LIKE 'quikvol%'
             ORDER BY as_of_date DESC, created_at DESC
             LIMIT 200`
        ).all();
        return Response.json({
            success: true,
            count: rows.results?.length || 0,
            data: rows.results || [],
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
