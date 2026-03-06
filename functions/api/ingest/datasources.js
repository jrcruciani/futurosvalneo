import {
    asNumber,
    createSourceFileRecord,
    jsonError,
    normalizeDate,
    parseHeatmapMetrics,
    parseMostActiveMetrics,
    parseDarkpoolMetrics,
    parseNinjaTicks,
    upsertOptionMetrics,
    upsertVolatilitySignals,
    upsertDarkpoolLevels,
    upsertMarketTicks,
} from '../_helpers.js';

const PREFIX = 'datasources/';

// Detection rules ordered from most specific to least specific
const MODULE_RULES = [
    { match: (n) => n.includes('most_active') && n.includes('call') && n.includes('oic'), type: 'cme', moduleName: 'most_active_calls_oic' },
    { match: (n) => n.includes('most_active') && n.includes('put') && n.includes('oic'), type: 'cme', moduleName: 'most_active_puts_oic' },
    { match: (n) => n.includes('most_active') && n.includes('call'), type: 'cme', moduleName: 'most_active_calls' },
    { match: (n) => n.includes('most_active') && n.includes('put'), type: 'cme', moduleName: 'most_active_puts' },
    { match: (n) => n.includes('oi_change') || n.includes('oichange'), type: 'cme', moduleName: 'oi_change_heatmap' },
    { match: (n) => n.includes('volume_heatmap') || n.includes('vol_heatmap'), type: 'cme', moduleName: 'volume_heatmap' },
    { match: (n) => n.includes('oi_heatmap'), type: 'cme', moduleName: 'oi_heatmap' },
    { match: (n) => n.includes('vol2vol'), type: 'cme', moduleName: 'vol2vol_intraday' },
    { match: (n) => n.includes('darkpool') || n.includes('dark_pool'), type: 'darkpool', moduleName: 'darkpool_levels' },
    { match: (n) => n.includes('quikvol'), type: 'quikvol', moduleName: 'quikvol' },
    { match: (n) => n.includes('nq_price') || (n.includes('nq') && n.endsWith('.txt')), type: 'ninja', moduleName: 'nq_price' },
];

function detectModule(fileName) {
    const n = fileName.toLowerCase();
    return MODULE_RULES.find((rule) => rule.match(n)) || null;
}

function extractDate(key) {
    const match = key.match(/(\d{4}[-_]\d{2}[-_]\d{2})/);
    if (!match) return null;
    return normalizeDate(match[1].replace(/_/g, '-'), null);
}

function simpleHash(text) {
    let h = 5381;
    const limit = Math.min(text.length, 4000);
    for (let i = 0; i < limit; i++) {
        h = ((h << 5) + h) + text.charCodeAt(i);
        h |= 0;
    }
    return `${Math.abs(h).toString(16)}-${text.length}`;
}

async function getLastConversionRatio(db) {
    const row = await db.prepare(
        `SELECT conversion_ratio FROM darkpool_levels ORDER BY as_of_date DESC, created_at DESC LIMIT 1`
    ).first();
    return row?.conversion_ratio ?? 40.35;
}

async function alreadyProcessed(db, filePath, asOfDate) {
    const row = await db.prepare(
        `SELECT id FROM source_files WHERE file_path = ? AND as_of_date = ? LIMIT 1`
    ).bind(filePath, asOfDate).first();
    return !!row;
}

async function processOne(db, key, content, today, conversionRatio) {
    const fileName = key.split('/').pop();
    const detected = detectModule(fileName);
    if (!detected) {
        return { key, status: 'skipped', reason: 'tipo no reconocido' };
    }

    const asOfDate = extractDate(key) || today;
    const { type, moduleName } = detected;

    if (await alreadyProcessed(db, key, asOfDate)) {
        return { key, status: 'skipped', reason: `ya procesado para ${asOfDate}` };
    }

    const mimeType = fileName.endsWith('.csv') ? 'text/csv' : 'text/plain';

    const source = await createSourceFileRecord(db, {
        sourceName: 'r2_datasources',
        moduleName,
        fileName,
        filePath: key,
        mimeType,
        fileSize: content.length,
        checksum: simpleHash(content),
        asOfDate,
        status: 'ingested',
        metadata: { channel: 'api/ingest/datasources' },
    });

    let metricsInserted = 0;
    let signalsInserted = 0;

    if (type === 'ninja') {
        const price = asNumber(content.trim());
        const ticks = price != null
            ? parseNinjaTicks({ nqPrice: price, tickTime: new Date().toISOString(), volume: 0 })
            : parseNinjaTicks({ csv: content });
        metricsInserted = await upsertMarketTicks(db, ticks, source.id);

    } else if (type === 'darkpool') {
        const levels = parseDarkpoolMetrics(content, conversionRatio, asOfDate);
        metricsInserted = await upsertDarkpoolLevels(db, levels, source.id);

    } else if (type === 'cme') {
        if (moduleName.endsWith('heatmap')) {
            const metrics = parseHeatmapMetrics(content, moduleName, asOfDate);
            metricsInserted = await upsertOptionMetrics(db, metrics, source.id);
        } else if (moduleName === 'vol2vol_intraday') {
            const signals = [{ asOfDate, signalType: 'vol2vol_intraday_uploaded', level: -1, value: 1, notes: 'Cargado desde datasources/' }];
            signalsInserted = await upsertVolatilitySignals(db, signals, source.id);
        } else {
            const parsed = parseMostActiveMetrics(content, moduleName, asOfDate);
            metricsInserted = await upsertOptionMetrics(db, parsed.metrics, source.id);
            signalsInserted = await upsertVolatilitySignals(db, parsed.signals, source.id);
        }

    } else if (type === 'quikvol') {
        const signals = [{ asOfDate, signalType: 'quikvol_uploaded', level: -1, value: 1, notes: 'Cargado desde datasources/' }];
        signalsInserted = await upsertVolatilitySignals(db, signals, source.id);
    }

    return { key, status: 'ok', moduleName, asOfDate, metricsInserted, signalsInserted };
}

// POST: sync all files in datasources/ into D1
export async function onRequestPost(context) {
    const { env } = context;
    const db = env.DB;
    try {
        const body = await context.request.json().catch(() => ({}));
        const force = !!body.force;
        const today = new Date().toISOString().slice(0, 10);

        const listed = await env.SOURCES.list({ prefix: PREFIX });
        const objects = listed.objects || [];

        if (!objects.length) {
            return Response.json({ success: true, message: `Sin archivos en ${PREFIX}`, results: [] });
        }

        const conversionRatio = await getLastConversionRatio(db);
        const results = [];

        for (const obj of objects) {
            try {
                // Force: delete existing record so it re-processes
                if (force) {
                    await db.prepare(
                        `DELETE FROM source_files WHERE file_path = ?`
                    ).bind(obj.key).run();
                }

                const r2obj = await env.SOURCES.get(obj.key);
                if (!r2obj) {
                    results.push({ key: obj.key, status: 'error', reason: 'objeto no encontrado en R2' });
                    continue;
                }

                const content = await r2obj.text();
                const result = await processOne(db, obj.key, content, today, conversionRatio);
                results.push(result);
            } catch (err) {
                results.push({ key: obj.key, status: 'error', reason: err.message });
            }
        }

        const ok = results.filter((r) => r.status === 'ok').length;
        const skipped = results.filter((r) => r.status === 'skipped').length;
        const errors = results.filter((r) => r.status === 'error').length;

        return Response.json({ success: true, today, force, summary: { ok, skipped, errors }, results });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}

// GET: list files currently in datasources/ with detection info
export async function onRequestGet(context) {
    const { env } = context;
    try {
        const listed = await env.SOURCES.list({ prefix: PREFIX });
        const objects = (listed.objects || []).map((obj) => {
            const fileName = obj.key.split('/').pop();
            const detected = detectModule(fileName);
            return {
                key: obj.key,
                fileName,
                size: obj.size,
                uploaded: obj.uploaded,
                detectedModule: detected?.moduleName || null,
                detectedType: detected?.type || null,
            };
        });
        return Response.json({ success: true, prefix: PREFIX, count: objects.length, objects });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
