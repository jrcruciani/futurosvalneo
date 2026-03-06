import {
    createSourceFileRecord,
    jsonError,
    normalizeDate,
    parseHeatmapMetrics,
    parseMostActiveMetrics,
    storeRawSource,
    upsertOptionMetrics,
    upsertVolatilitySignals,
} from '../_helpers.js';

const SUPPORTED_CME_MODULES = new Set([
    'oi_change_heatmap',
    'volume_heatmap',
    'oi_heatmap',
    'most_active_calls',
    'most_active_puts',
    'most_active_calls_oic',
    'most_active_puts_oic',
    'vol2vol_intraday',
]);

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        const body = await context.request.json();
        const moduleName = String(body.moduleName || '').trim();
        const fileName = String(body.fileName || '').trim();
        const content = String(body.content || '');
        const mimeType = String(body.mimeType || 'text/csv');
        const asOfDate = normalizeDate(body.asOfDate, new Date().toISOString().slice(0, 10));

        if (!SUPPORTED_CME_MODULES.has(moduleName)) {
            return jsonError('moduleName no soportado para ingesta CME.', 400, {
                supported: [...SUPPORTED_CME_MODULES],
            });
        }
        if (!fileName) return jsonError('fileName es obligatorio.');
        if (!content) return jsonError('content es obligatorio (texto CSV).');

        const lowerName = fileName.toLowerCase();
        const isBinary = mimeType.startsWith('image/')
            || mimeType === 'application/pdf'
            || lowerName.endsWith('.pdf')
            || lowerName.endsWith('.jpg')
            || lowerName.endsWith('.jpeg')
            || lowerName.endsWith('.png');

        const stored = await storeRawSource(context.env, {
            category: isBinary ? 'cme/pdf' : (moduleName === 'vol2vol_intraday' ? 'quikvol/images' : 'cme/csv'),
            fileName,
            content,
            mimeType,
        });

        const source = await createSourceFileRecord(db, {
            sourceName: 'cme',
            moduleName,
            fileName,
            filePath: stored.key,
            mimeType,
            fileSize: stored.size,
            checksum: stored.checksum,
            asOfDate,
            metadata: { channel: 'api/ingest/cme' },
        });

        let metricRows = [];
        let signalRows = [];

        if (isBinary) {
            signalRows.push({
                asOfDate,
                signalType: `${moduleName}_binary_uploaded`,
                level: -1,
                value: 1,
                notes: `Archivo binario adjunto para ${moduleName}`,
            });
        } else if (moduleName === 'vol2vol_intraday') {
            signalRows.push({
                asOfDate,
                signalType: 'vol2vol_intraday_uploaded',
                level: -1,
                value: 1,
                notes: 'Archivo cargado para módulo 4',
            });
        } else if (moduleName.endsWith('heatmap')) {
            metricRows = parseHeatmapMetrics(content, moduleName, asOfDate);
        } else {
            const parsed = parseMostActiveMetrics(content, moduleName, asOfDate);
            metricRows = parsed.metrics;
            signalRows = parsed.signals;
        }

        const metricsInserted = await upsertOptionMetrics(db, metricRows, source.id);
        const signalsInserted = await upsertVolatilitySignals(db, signalRows, source.id);

        return Response.json({
            success: true,
            sourceFileId: source.id,
            moduleName,
            fileName,
            mimeType,
            asOfDate,
            metricsInserted,
            signalsInserted,
            parsed: !isBinary,
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
            `SELECT id, module_name, file_name, as_of_date, created_at
             FROM source_files
             WHERE source_name = 'cme'
             ORDER BY created_at DESC
             LIMIT 50`
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
