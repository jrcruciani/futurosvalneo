const HEATMAP_METRIC_BY_MODULE = {
    oi_change_heatmap: 'oi_change',
    volume_heatmap: 'volume',
    oi_heatmap: 'open_interest',
};

const MOST_ACTIVE_MODULES = new Set([
    'most_active_calls',
    'most_active_puts',
    'most_active_calls_oic',
    'most_active_puts_oic',
]);

const REQUIRED_FILES = ['NQ_Price.txt', '13.darkpool_levels_QQQ.csv'];

export function jsonError(message, status = 400, extra = {}) {
    return Response.json({ error: message, ...extra }, { status });
}

export function asNumber(value) {
    if (value == null) return null;
    const raw = String(value).replace(/\u2212/g, '-').trim();
    if (!raw) return null;

    const suffixMatch = raw.match(/([kmb])$/i);
    let multiplier = 1;
    if (suffixMatch) {
        const suffix = suffixMatch[1].toLowerCase();
        if (suffix === 'k') multiplier = 1_000;
        if (suffix === 'm') multiplier = 1_000_000;
        if (suffix === 'b') multiplier = 1_000_000_000;
    }

    const cleaned = raw
        .replace(/["$,%\s]/g, '')
        .replace(/(k|m|b)$/i, '')
        .trim();
    if (!cleaned) return null;
    const n = Number(cleaned);
    return Number.isFinite(n) ? n * multiplier : null;
}

export function normalizeDate(input, fallback = null) {
    if (!input) return fallback;
    const raw = String(input).trim().toLowerCase();
    if (!raw) return fallback;

    const monthMap = {
        ene: 1, enero: 1, jan: 1, january: 1,
        feb: 2, febrero: 2, february: 2,
        mar: 3, marzo: 3, march: 3,
        abr: 4, abril: 4, apr: 4, april: 4,
        may: 5, mayo: 5,
        jun: 6, junio: 6, june: 6,
        jul: 7, julio: 7, july: 7,
        ago: 8, agosto: 8, aug: 8, august: 8,
        sep: 9, sept: 9, septiembre: 9, september: 9,
        oct: 10, octubre: 10, october: 10,
        nov: 11, noviembre: 11, november: 11,
        dic: 12, diciembre: 12, dec: 12, december: 12,
    };

    const slash = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{2,4})$/);
    if (slash) {
        const mm = Number(slash[1]);
        const dd = Number(slash[2]);
        const yy = Number(slash[3].length === 2 ? `20${slash[3]}` : slash[3]);
        return `${yy}-${String(mm).padStart(2, '0')}-${String(dd).padStart(2, '0')}`;
    }

    const monthDay = raw.replace('.', '').match(/^([a-záéíóúñ]+)\s+(\d{1,2})(?:,?\s+(\d{2,4}))?$/);
    if (monthDay) {
        const monthToken = monthDay[1];
        const month = monthMap[monthToken];
        if (!month) return fallback;
        const day = Number(monthDay[2]);
        const year = monthDay[3] ? Number(monthDay[3].length === 2 ? `20${monthDay[3]}` : monthDay[3]) : new Date().getUTCFullYear();
        return `${year}-${String(month).padStart(2, '0')}-${String(day).padStart(2, '0')}`;
    }

    const iso = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
    if (iso) return raw;
    return fallback;
}

export function csvToRows(text) {
    const rows = [];
    let row = [];
    let cell = '';
    let inQuotes = false;
    const input = String(text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');

    for (let i = 0; i < input.length; i++) {
        const ch = input[i];
        const next = input[i + 1];

        if (ch === '"') {
            if (inQuotes && next === '"') {
                cell += '"';
                i++;
            } else {
                inQuotes = !inQuotes;
            }
            continue;
        }

        if (ch === ',' && !inQuotes) {
            row.push(cell.trim());
            cell = '';
            continue;
        }

        if (ch === '\n' && !inQuotes) {
            row.push(cell.trim());
            rows.push(row);
            row = [];
            cell = '';
            continue;
        }

        cell += ch;
    }

    if (cell.length > 0 || row.length > 0) {
        row.push(cell.trim());
        rows.push(row);
    }

    return rows.filter((r) => r.some((c) => c !== ''));
}

function checksum32(text) {
    let hash = 5381;
    for (let i = 0; i < text.length; i++) {
        hash = ((hash << 5) + hash) + text.charCodeAt(i);
        hash |= 0;
    }
    return Math.abs(hash).toString(16);
}

function safeName(fileName) {
    return String(fileName || 'source.dat').replace(/[^a-zA-Z0-9._-]+/g, '-');
}

export async function storeRawSource(env, { category, fileName, content, mimeType = 'text/plain' }) {
    if (!env.SOURCES) {
        throw new Error('R2 bucket binding SOURCES no está configurado.');
    }

    const now = new Date();
    const datePath = `${now.getUTCFullYear()}/${String(now.getUTCMonth() + 1).padStart(2, '0')}/${String(now.getUTCDate()).padStart(2, '0')}`;
    const checksum = checksum32(content);
    const key = `${category}/${datePath}/${Date.now()}-${safeName(fileName)}`;
    await env.SOURCES.put(key, content, {
        httpMetadata: { contentType: mimeType },
        customMetadata: { checksum },
    });
    return {
        key,
        checksum,
        size: content.length,
    };
}

export async function createSourceFileRecord(db, payload) {
    const row = await db.prepare(
        `INSERT INTO source_files (
            source_name, module_name, file_name, file_path, mime_type, file_size, checksum, as_of_date, status, metadata_json
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        RETURNING id, created_at`
    ).bind(
        payload.sourceName,
        payload.moduleName,
        payload.fileName,
        payload.filePath,
        payload.mimeType || 'text/plain',
        payload.fileSize || 0,
        payload.checksum || '',
        payload.asOfDate || null,
        payload.status || 'ingested',
        JSON.stringify(payload.metadata || {})
    ).first();
    return row;
}

export function parseHeatmapMetrics(csvText, moduleName, defaultAsOfDate = null) {
    const metricName = HEATMAP_METRIC_BY_MODULE[moduleName];
    if (!metricName) {
        throw new Error(`Módulo CME no soportado para heatmap: ${moduleName}`);
    }

    const rows = csvToRows(csvText);
    if (rows.length < 2) {
        throw new Error('CSV heatmap inválido: sin filas de datos');
    }

    const headers = rows[0];
    const columns = headers.map((header, idx) => {
        if (idx === 0) return null;
        const match = String(header).trim().match(/^(Call|Put)\s+(.+)$/i);
        if (!match) return null;
        return {
            idx,
            side: match[1].toLowerCase() === 'call' ? 'call' : 'put',
            asOfDate: normalizeDate(match[2], defaultAsOfDate),
        };
    }).filter(Boolean);

    const out = [];
    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const strike = asNumber(row[0]);
        if (strike == null) continue;
        for (const col of columns) {
            const value = asNumber(row[col.idx]);
            if (value == null) continue;
            out.push({
                asOfDate: col.asOfDate || defaultAsOfDate || new Date().toISOString().slice(0, 10),
                moduleName,
                strike,
                optionSide: col.side,
                metricName,
                metricValue: value,
            });
        }
    }
    return out;
}

export function parseMostActiveMetrics(csvText, moduleName, defaultAsOfDate = null) {
    if (!MOST_ACTIVE_MODULES.has(moduleName)) {
        throw new Error(`Módulo Most Active no soportado: ${moduleName}`);
    }

    const rows = csvToRows(csvText);
    if (rows.length < 2) {
        throw new Error('CSV Most Active inválido: sin filas de datos');
    }

    const side = moduleName.includes('calls') ? 'call' : 'put';
    const out = [];
    const signalRows = [];
    const asOfDate = defaultAsOfDate || new Date().toISOString().slice(0, 10);
    const headerCells = rows[0].map((c) => String(c || '').toLowerCase());
    const hasStrikeRows = headerCells.some((c) => c.includes('strike'));

    if (!hasStrikeRows) {
        const summaryRow = rows[2] || [];
        const summaries = [
            ['total_volume', asNumber(summaryRow[0])],
            ['total_open_interest', asNumber(summaryRow[1])],
            ['strikes_up', asNumber(summaryRow[2])],
            ['strikes_down', asNumber(summaryRow[3])],
            ['net_change', asNumber(summaryRow[4])],
        ];
        for (const [signalType, value] of summaries) {
            if (value == null) continue;
            signalRows.push({
                asOfDate,
                signalType: `${moduleName}_${signalType}`,
                level: null,
                value,
                notes: `Resumen ${moduleName}`,
            });
        }
        return { metrics: out, signals: signalRows };
    }

    for (let i = 0; i < rows.length; i++) {
        const row = rows[i];
        const strike = asNumber(row[0]);
        if (strike == null) continue;

        const vol = asNumber(row[1]);
        const oiPrev = asNumber(row[2]);
        const oiCurr = asNumber(row[3]);
        const oiChg = asNumber(row[4]);

        if (vol != null) {
            out.push({ asOfDate, moduleName, strike, optionSide: side, metricName: 'most_active_volume', metricValue: vol });
        }
        if (oiCurr != null) {
            out.push({ asOfDate, moduleName, strike, optionSide: side, metricName: 'most_active_open_interest', metricValue: oiCurr });
        }
        if (oiChg != null) {
            out.push({ asOfDate, moduleName, strike, optionSide: side, metricName: 'most_active_open_interest_change', metricValue: oiChg });
        }
        if (oiPrev != null) {
            out.push({ asOfDate, moduleName, strike, optionSide: side, metricName: 'most_active_open_interest_prev', metricValue: oiPrev });
        }
    }

    // Archivos *_OIC pueden incluir resumen global en filas con "Total Volume"
    for (const row of rows) {
        if (!row[0] || !String(row[0]).toLowerCase().includes('total volume')) continue;
        const totalVolume = asNumber(rows[2]?.[0]);
        const totalOI = asNumber(rows[2]?.[1]);
        const strikesUp = asNumber(rows[2]?.[2]);
        const strikesDown = asNumber(rows[2]?.[3]);
        const netChg = asNumber(rows[2]?.[4]);
        const summaries = [
            ['total_volume', totalVolume],
            ['total_open_interest', totalOI],
            ['strikes_up', strikesUp],
            ['strikes_down', strikesDown],
            ['net_change', netChg],
        ];
        for (const [signalType, value] of summaries) {
            if (value == null) continue;
            signalRows.push({
                asOfDate,
                signalType: `${moduleName}_${signalType}`,
                level: null,
                value,
                notes: `Resumen ${moduleName}`,
            });
        }
        break;
    }

    return { metrics: out, signals: signalRows };
}

export function parseDarkpoolMetrics(csvText, conversionRatio, defaultAsOfDate = null) {
    const rows = csvToRows(csvText);
    if (rows.length < 2) throw new Error('CSV DarkPool inválido: sin datos');

    const headers = rows[0].map((h) => String(h || '').toLowerCase());
    const findIdx = (needle) => headers.findIndex((h) => h.includes(needle));
    const levelIdx = findIdx('level') >= 0 ? findIdx('level') : (findIdx('price') >= 0 ? findIdx('price') : 0);
    const volumeIdx = findIdx('volume');
    const sideIdx = findIdx('side') >= 0 ? findIdx('side') : findIdx('type');
    const asOfDate = defaultAsOfDate || new Date().toISOString().slice(0, 10);

    const out = [];
    for (let i = 1; i < rows.length; i++) {
        const row = rows[i];
        const qqqLevel = asNumber(row[levelIdx]);
        if (qqqLevel == null) continue;
        const volume = volumeIdx >= 0 ? asNumber(row[volumeIdx]) : null;
        const side = sideIdx >= 0 ? String(row[sideIdx] || '').toLowerCase() : '';
        out.push({
            asOfDate,
            qqqLevel,
            nqLevel: qqqLevel * conversionRatio,
            conversionRatio,
            volume: volume == null ? 0 : volume,
            side,
        });
    }
    return out;
}

export async function upsertOptionMetrics(db, metrics, sourceFileId) {
    if (!metrics.length) return 0;
    const stmts = metrics.map((m) => db.prepare(
        `INSERT INTO option_chain_raw (
            as_of_date, module_name, strike, option_side, metric_name, metric_value, source_file_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(as_of_date, module_name, strike, option_side, metric_name)
        DO UPDATE SET metric_value = excluded.metric_value, source_file_id = excluded.source_file_id, created_at = datetime('now')`
    ).bind(
        m.asOfDate,
        m.moduleName,
        m.strike,
        m.optionSide,
        m.metricName,
        m.metricValue,
        sourceFileId
    ));

    for (let i = 0; i < stmts.length; i += 80) {
        await db.batch(stmts.slice(i, i + 80));
    }
    return metrics.length;
}

export async function upsertVolatilitySignals(db, signals, sourceFileId) {
    if (!signals.length) return 0;
    const stmts = signals.map((s) => db.prepare(
        `INSERT INTO volatility_signals (
            as_of_date, signal_type, level, value, notes, source_file_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(as_of_date, signal_type, level)
        DO UPDATE SET value = excluded.value, notes = excluded.notes, source_file_id = excluded.source_file_id, created_at = datetime('now')`
    ).bind(
        s.asOfDate,
        s.signalType,
        s.level == null ? -1 : s.level,
        s.value,
        s.notes || '',
        sourceFileId
    ));

    for (let i = 0; i < stmts.length; i += 80) {
        await db.batch(stmts.slice(i, i + 80));
    }
    return signals.length;
}

export async function upsertDarkpoolLevels(db, levels, sourceFileId) {
    if (!levels.length) return 0;
    const stmts = levels.map((l) => db.prepare(
        `INSERT INTO darkpool_levels (
            as_of_date, qqq_level, nq_level, conversion_ratio, volume, side, source_file_id, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(as_of_date, qqq_level, side)
        DO UPDATE SET nq_level = excluded.nq_level, conversion_ratio = excluded.conversion_ratio, volume = excluded.volume, source_file_id = excluded.source_file_id, created_at = datetime('now')`
    ).bind(
        l.asOfDate,
        l.qqqLevel,
        l.nqLevel,
        l.conversionRatio,
        l.volume || 0,
        l.side || '',
        sourceFileId
    ));

    for (let i = 0; i < stmts.length; i += 80) {
        await db.batch(stmts.slice(i, i + 80));
    }
    return levels.length;
}

export async function upsertMarketTicks(db, ticks, sourceFileId) {
    if (!ticks.length) return 0;
    const stmts = ticks.map((t) => db.prepare(
        `INSERT INTO market_ticks (
            tick_time, nq_price, volume, source_file_id, metadata_json, created_at
        ) VALUES (?, ?, ?, ?, ?, datetime('now'))
        ON CONFLICT(tick_time)
        DO UPDATE SET nq_price = excluded.nq_price, volume = excluded.volume, source_file_id = excluded.source_file_id, metadata_json = excluded.metadata_json, created_at = datetime('now')`
    ).bind(
        t.tickTime,
        t.nqPrice,
        t.volume || 0,
        sourceFileId,
        JSON.stringify(t.metadata || {})
    ));

    for (let i = 0; i < stmts.length; i += 80) {
        await db.batch(stmts.slice(i, i + 80));
    }
    return ticks.length;
}

export function parseNinjaTicks(body) {
    if (Array.isArray(body.ticks)) {
        return body.ticks
            .map((t) => ({
                tickTime: normalizeDateTime(t.tickTime || t.timestamp),
                nqPrice: asNumber(t.nqPrice || t.price),
                volume: asNumber(t.volume) || 0,
                metadata: t.metadata || {},
            }))
            .filter((t) => t.tickTime && t.nqPrice != null);
    }

    if (body.csv) {
        const rows = csvToRows(body.csv);
        if (!rows.length) return [];
        const headers = rows[0].map((h) => String(h || '').toLowerCase());
        const idxTime = headers.findIndex((h) => h.includes('time') || h.includes('timestamp'));
        const idxPrice = headers.findIndex((h) => h.includes('price') || h.includes('nq'));
        const idxVolume = headers.findIndex((h) => h.includes('vol'));
        return rows.slice(1).map((row) => ({
            tickTime: normalizeDateTime(row[idxTime >= 0 ? idxTime : 0]),
            nqPrice: asNumber(row[idxPrice >= 0 ? idxPrice : 1]),
            volume: asNumber(row[idxVolume >= 0 ? idxVolume : 2]) || 0,
            metadata: { source: 'csv' },
        })).filter((t) => t.tickTime && t.nqPrice != null);
    }

    const nqPrice = asNumber(body.nqPrice || body.price);
    if (nqPrice == null) return [];
    return [{
        tickTime: normalizeDateTime(body.tickTime || body.timestamp || new Date().toISOString()),
        nqPrice,
        volume: asNumber(body.volume) || 0,
        metadata: { source: 'manual' },
    }];
}

function normalizeDateTime(value) {
    if (!value) return null;
    const d = new Date(value);
    if (Number.isNaN(d.getTime())) return null;
    return d.toISOString();
}

function computeMaxPain(strikes, callOI, putOI) {
    if (!strikes.length) return null;
    let minPain = Number.POSITIVE_INFINITY;
    let best = strikes[0];
    for (const test of strikes) {
        let pain = 0;
        for (let i = 0; i < strikes.length; i++) {
            if (test > strikes[i]) pain += (test - strikes[i]) * (callOI[i] || 0);
            if (test < strikes[i]) pain += (strikes[i] - test) * (putOI[i] || 0);
        }
        if (pain < minPain) {
            minPain = pain;
            best = test;
        }
    }
    return best;
}

async function loadLatestMetricRows(db, moduleName, metricName) {
    const rows = await db.prepare(
        `SELECT strike, option_side, metric_value, as_of_date
         FROM option_chain_raw
         WHERE module_name = ? AND metric_name = ?
         AND as_of_date = (
            SELECT MAX(as_of_date) FROM option_chain_raw WHERE module_name = ? AND metric_name = ?
         )
         ORDER BY strike ASC`
    ).bind(moduleName, metricName, moduleName, metricName).all();
    return rows.results || [];
}

export async function getModuleStatus(db) {
    const sourceRows = await db.prepare(
        `SELECT module_name, COUNT(*) as file_count, MAX(created_at) as last_ingested
         FROM source_files
         GROUP BY module_name`
    ).all();

    const map = new Map((sourceRows.results || []).map((r) => [r.module_name, r]));
    const expected = [
        {
            label: 'Módulo 1 · OI Change',
            moduleName: 'oi_change_heatmap',
            requiredModules: ['oi_change_heatmap', 'volume_heatmap'],
        },
        {
            label: 'Módulo 2 · Most Active',
            moduleName: 'most_active',
            requiredModules: ['most_active_calls', 'most_active_puts', 'most_active_calls_oic', 'most_active_puts_oic'],
            minRequired: 2,
        },
        {
            label: 'Módulo 3 · OI Heatmap',
            moduleName: 'oi_heatmap',
            requiredModules: ['oi_heatmap'],
        },
        {
            label: 'Módulo 4 · Vol2Vol',
            moduleName: 'vol2vol_intraday',
            requiredModules: ['vol2vol_intraday'],
        },
        {
            label: 'Módulo 5 · QuikVol',
            moduleName: 'quikvol',
            requiredModules: ['quikvol'],
        },
        {
            label: 'Módulo 6 · DarkPool',
            moduleName: 'darkpool_levels',
            requiredModules: ['darkpool_levels'],
        },
    ];

    return expected.map((moduleDef) => {
        const loadedModuleNames = moduleDef.requiredModules.filter((name) => map.has(name));
        const loaded = moduleDef.requiredModules
            .map((name) => map.get(name))
            .filter(Boolean);
        const required = moduleDef.minRequired || moduleDef.requiredModules.length;
        const totalFiles = loaded.reduce((acc, row) => acc + Number(row.file_count || 0), 0);
        const latest = loaded.length
            ? loaded.map((row) => row.last_ingested).sort().reverse()[0]
            : null;

        const ready = loaded.length >= required;

        return {
            moduleName: moduleDef.moduleName,
            label: moduleDef.label,
            fileCount: totalFiles,
            lastIngested: latest,
            status: ready ? 'ready' : 'pending',
            requiredModules: moduleDef.requiredModules,
            loadedModules: loadedModuleNames,
        };
    });
}

export async function buildDashboardData(db) {
    const tick = await db.prepare(
        `SELECT tick_time, nq_price, volume
         FROM market_ticks
         ORDER BY tick_time DESC
         LIMIT 1`
    ).first();

    const ratioRow = await db.prepare(
        `SELECT conversion_ratio
         FROM darkpool_levels
         ORDER BY as_of_date DESC, created_at DESC
         LIMIT 1`
    ).first();

    const conversionRatio = ratioRow?.conversion_ratio || 40;
    const nqPrice = tick?.nq_price || null;
    const qqqPrice = nqPrice ? nqPrice / conversionRatio : null;

    const oiRows = await loadLatestMetricRows(db, 'oi_heatmap', 'open_interest');
    const oiChgRows = await loadLatestMetricRows(db, 'oi_change_heatmap', 'oi_change');
    const volRows = await loadLatestMetricRows(db, 'volume_heatmap', 'volume');

    const allStrikes = new Set();
    for (const row of [...oiRows, ...oiChgRows, ...volRows]) allStrikes.add(row.strike);
    const strikes = [...allStrikes].sort((a, b) => a - b);

    const indexByStrike = new Map(strikes.map((s, i) => [s, i]));
    const callOI = new Array(strikes.length).fill(0);
    const putOI = new Array(strikes.length).fill(0);
    const callOIChange = new Array(strikes.length).fill(0);
    const putOIChange = new Array(strikes.length).fill(0);
    const callVolume = new Array(strikes.length).fill(0);
    const putVolume = new Array(strikes.length).fill(0);

    const loadSideArrays = (rows, callArr, putArr) => {
        for (const row of rows) {
            const idx = indexByStrike.get(row.strike);
            if (idx == null) continue;
            if (row.option_side === 'call') callArr[idx] = row.metric_value;
            if (row.option_side === 'put') putArr[idx] = row.metric_value;
        }
    };

    loadSideArrays(oiRows, callOI, putOI);
    loadSideArrays(oiChgRows, callOIChange, putOIChange);
    loadSideArrays(volRows, callVolume, putVolume);

    const totalCallOI = callOI.reduce((a, b) => a + b, 0);
    const totalPutOI = putOI.reduce((a, b) => a + b, 0);
    const totalCallVolume = callVolume.reduce((a, b) => a + b, 0);
    const totalPutVolume = putVolume.reduce((a, b) => a + b, 0);
    const pcRatio = totalCallOI > 0 ? totalPutOI / totalCallOI : 0;

    const maxPainStrike = computeMaxPain(strikes, callOI, putOI);
    const callWallIdx = callOI.length ? callOI.indexOf(Math.max(...callOI)) : -1;
    const putWallIdx = putOI.length ? putOI.indexOf(Math.max(...putOI)) : -1;
    const attractionIdx = strikes.length
        ? strikes.map((_, i) => callOI[i] + putOI[i]).indexOf(Math.max(...strikes.map((_, i) => callOI[i] + putOI[i])))
        : -1;

    const mostActiveRows = await db.prepare(
        `SELECT strike,
            SUM(CASE WHEN option_side = 'call' AND metric_name = 'most_active_volume' THEN metric_value ELSE 0 END) AS callVol,
            SUM(CASE WHEN option_side = 'put' AND metric_name = 'most_active_volume' THEN metric_value ELSE 0 END) AS putVol,
            SUM(CASE WHEN option_side = 'call' AND metric_name = 'most_active_open_interest' THEN metric_value ELSE 0 END) AS callOI,
            SUM(CASE WHEN option_side = 'put' AND metric_name = 'most_active_open_interest' THEN metric_value ELSE 0 END) AS putOI,
            SUM(CASE WHEN option_side = 'call' AND metric_name = 'most_active_open_interest_change' THEN metric_value ELSE 0 END) AS callOIChg,
            SUM(CASE WHEN option_side = 'put' AND metric_name = 'most_active_open_interest_change' THEN metric_value ELSE 0 END) AS putOIChg
         FROM option_chain_raw
         WHERE module_name IN ('most_active_calls', 'most_active_puts', 'most_active_calls_oic', 'most_active_puts_oic')
         GROUP BY strike
         ORDER BY (callVol + putVol) DESC
         LIMIT 15`
    ).all();

    const mostActiveStrikes = (mostActiveRows.results || []).map((r) => ({
        strike: r.strike,
        nqStrike: Math.round(r.strike),
        callVol: Math.round(r.callVol || 0),
        putVol: Math.round(r.putVol || 0),
        totalVol: Math.round((r.callVol || 0) + (r.putVol || 0)),
        callOI: Math.round(r.callOI || 0),
        putOI: Math.round(r.putOI || 0),
        callOIChg: Math.round(r.callOIChg || 0),
        putOIChg: Math.round(r.putOIChg || 0),
    }));

    const missingInputs = [];
    const hasNqPriceFile = await db.prepare(
        `SELECT COUNT(*) as c FROM source_files WHERE file_name = ?`
    ).bind(REQUIRED_FILES[0]).first();
    const hasDarkpoolFile = await db.prepare(
        `SELECT COUNT(*) as c FROM source_files WHERE file_name = ?`
    ).bind(REQUIRED_FILES[1]).first();
    const darkpoolRows = await db.prepare(`SELECT COUNT(*) as c FROM darkpool_levels`).first();

    if (!(hasNqPriceFile?.c || nqPrice)) {
        missingInputs.push(REQUIRED_FILES[0]);
    }
    if (!(hasDarkpoolFile?.c || darkpoolRows?.c)) {
        missingInputs.push(REQUIRED_FILES[1]);
    }
    if (!nqPrice) missingInputs.push('NQ price realtime (tick NinjaTrader)');

    return {
        timestamp: new Date().toISOString(),
        market: {
            nqPrice: nqPrice ? Number(nqPrice.toFixed(2)) : null,
            qqqPrice: qqqPrice ? Number(qqqPrice.toFixed(2)) : null,
            conversionRatio: Number(conversionRatio.toFixed(4)),
            lastTickTime: tick?.tick_time || null,
            tickVolume: tick?.volume || 0,
        },
        maxPain: {
            strike: maxPainStrike,
            nqEquivalent: maxPainStrike,
        },
        expectedMove: {
            nqDown: putWallIdx >= 0 ? strikes[putWallIdx] : null,
            nqUp: callWallIdx >= 0 ? strikes[callWallIdx] : null,
            movePercent: nqPrice && callWallIdx >= 0 && putWallIdx >= 0
                ? Math.abs((strikes[callWallIdx] - strikes[putWallIdx]) / nqPrice) * 100
                : 0,
        },
        oiDistribution: {
            strikes,
            nqStrikes: strikes.map((s) => Math.round(s)),
            callOI,
            putOI,
            callOIChange,
            putOIChange,
            callVolume,
            putVolume,
        },
        mostActiveStrikes,
        summary: {
            totalCallOI: Math.round(totalCallOI),
            totalPutOI: Math.round(totalPutOI),
            totalCallVolume: Math.round(totalCallVolume),
            totalPutVolume: Math.round(totalPutVolume),
            pcRatio: Number(pcRatio.toFixed(3)),
            callWall: callWallIdx >= 0 ? strikes[callWallIdx] : null,
            putWall: putWallIdx >= 0 ? strikes[putWallIdx] : null,
            attractionStrike: attractionIdx >= 0 ? strikes[attractionIdx] : null,
            netOIChange: Math.round(callOIChange.reduce((a, b) => a + b, 0) + putOIChange.reduce((a, b) => a + b, 0)),
        },
        moduleStatus: await getModuleStatus(db),
        missingInputs,
    };
}

export async function getPromptBundle(requestUrl, count = 11) {
    const base = new URL(requestUrl).origin;
    const prompts = [];
    for (let i = 1; i <= count; i++) {
        const url = `${base}/prompts/Prompt${i}.txt`;
        const res = await fetch(url);
        if (!res.ok) {
            prompts.push({ id: i, text: '', missing: true });
            continue;
        }
        prompts.push({ id: i, text: await res.text(), missing: false });
    }
    return prompts;
}
