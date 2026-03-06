import {
    asNumber,
    createSourceFileRecord,
    jsonError,
    normalizeDate,
    parseDarkpoolMetrics,
    storeRawSource,
    upsertDarkpoolLevels,
} from '../_helpers.js';

function resolveConversionRatio(body) {
    const direct = asNumber(body.conversionRatio);
    if (direct && direct > 0) return direct;
    const nq = asNumber(body.nqPrice);
    const qqq = asNumber(body.qqqPrice);
    if (nq && qqq) return nq / qqq;
    return null;
}

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        const body = await context.request.json();
        const fileName = String(body.fileName || '').trim();
        const content = String(body.content || '');
        const asOfDate = normalizeDate(body.asOfDate, new Date().toISOString().slice(0, 10));
        const conversionRatio = resolveConversionRatio(body);

        if (!fileName) return jsonError('fileName es obligatorio.');
        if (!content) return jsonError('content es obligatorio (CSV DarkPool).');
        if (!conversionRatio || conversionRatio <= 0) {
            return jsonError('Debes enviar conversionRatio (o nqPrice y qqqPrice) para QQQ -> NQ.');
        }

        const stored = await storeRawSource(context.env, {
            category: 'darkpool',
            fileName,
            content,
            mimeType: 'text/csv',
        });

        const source = await createSourceFileRecord(db, {
            sourceName: 'darkpool',
            moduleName: 'darkpool_levels',
            fileName,
            filePath: stored.key,
            mimeType: 'text/csv',
            fileSize: stored.size,
            checksum: stored.checksum,
            asOfDate,
            metadata: { conversionRatio, channel: 'api/ingest/darkpool' },
        });

        const levels = parseDarkpoolMetrics(content, conversionRatio, asOfDate);
        const inserted = await upsertDarkpoolLevels(db, levels, source.id);

        return Response.json({
            success: true,
            sourceFileId: source.id,
            levelsInserted: inserted,
            conversionRatio,
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
            `SELECT as_of_date, qqq_level, nq_level, conversion_ratio, volume, side
             FROM darkpool_levels
             ORDER BY as_of_date DESC, volume DESC
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
