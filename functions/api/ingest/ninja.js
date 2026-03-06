import {
    asNumber,
    createSourceFileRecord,
    jsonError,
    parseNinjaTicks,
    storeRawSource,
    upsertMarketTicks,
} from '../_helpers.js';

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        const body = await context.request.json();
        const fileName = String(body.fileName || '').trim();
        const content = body.content == null ? '' : String(body.content);

        let ticks = [];
        let rawPayload = '';
        let rawName = fileName || `ninja-${Date.now()}.json`;
        let mimeType = 'application/json';

        if (content) {
            rawPayload = content;
            if (fileName.toLowerCase().endsWith('.txt')) {
                const price = asNumber(content);
                if (price == null) return jsonError('NQ_Price.txt no contiene un número válido.');
                ticks = parseNinjaTicks({
                    nqPrice: price,
                    tickTime: body.tickTime || new Date().toISOString(),
                    volume: body.volume || 0,
                });
                mimeType = 'text/plain';
            } else if (fileName.toLowerCase().endsWith('.csv')) {
                ticks = parseNinjaTicks({ csv: content });
                mimeType = 'text/csv';
            } else {
                ticks = parseNinjaTicks(body);
            }
        } else {
            ticks = parseNinjaTicks(body);
            rawPayload = JSON.stringify({ ticks }, null, 2);
        }

        if (!ticks.length) {
            return jsonError('No se detectaron ticks válidos. Envía nqPrice/tickTime o CSV con time,price,volume.');
        }

        const stored = await storeRawSource(context.env, {
            category: 'ninjatrader/realtime',
            fileName: rawName,
            content: rawPayload,
            mimeType,
        });

        const source = await createSourceFileRecord(db, {
            sourceName: 'ninja',
            moduleName: 'nq_price',
            fileName: rawName,
            filePath: stored.key,
            mimeType,
            fileSize: stored.size,
            checksum: stored.checksum,
            asOfDate: new Date().toISOString().slice(0, 10),
            metadata: { channel: 'api/ingest/ninja' },
        });

        const inserted = await upsertMarketTicks(db, ticks, source.id);

        return Response.json({
            success: true,
            sourceFileId: source.id,
            ticksInserted: inserted,
            latestTick: ticks[ticks.length - 1],
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
            `SELECT tick_time, nq_price, volume, created_at
             FROM market_ticks
             ORDER BY tick_time DESC
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
