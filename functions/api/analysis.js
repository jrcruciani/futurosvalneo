import { buildDashboardData, jsonError } from './_helpers.js';

const RUN_TYPE = 'prompt_orchestrator';
const PROMPT_BUNDLE = [
    {
        id: 1,
        text: 'Marco metodológico: para cada dataset explicar qué se busca, cómo se interpreta, su importancia en liquidez y confluencia Smart Money.',
    },
    {
        id: 2,
        text: 'Validación cruzada de OI Change Heatmap con CSV + precio NQ actual, priorizando niveles críticos intradía.',
    },
    {
        id: 3,
        text: 'Profundización de los niveles más inusuales: naturaleza Call/Put, comportamiento del precio, anomalías ITM y sesgo operativo.',
    },
    {
        id: 4,
        text: 'Módulo 1 OI Change: zonas de resistencia/soporte, relación Volumen vs OI Change y diagnóstico para la sesión.',
    },
    {
        id: 5,
        text: 'Módulo 2 Most Active Strikes: net change global, dinero nuevo vs liquidación, murallas y anomalías institucionales.',
    },
    {
        id: 6,
        text: 'Verificación CSV de Most Active (calls/puts/OIC) contra PDF y detección minuciosa de strikes con VOL/CHG relevantes.',
    },
    {
        id: 7,
        text: 'Módulo 3 OI Heatmap: persistencia temporal, call/put walls, anomalías ITM y mapa operativo compra/venta.',
    },
    {
        id: 8,
        text: 'Validación adicional del OI Heatmap CSV contra PDF para evitar omisiones de niveles significativos.',
    },
    {
        id: 9,
        text: 'Módulo 4 Vol2Vol Intraday: actualizar mapa operativo con zonas de entrada long/short y razón técnica.',
    },
    {
        id: 10,
        text: 'Módulo 5 QuikVol: lectura de Implied vs Actual, Diff y conclusiones institucionales (muralla, suelo defensivo, sentimiento).',
    },
    {
        id: 11,
        text: 'Módulo 6 DarkPool: convertir niveles QQQ->NQ, interpretar concentración de volumen y actualizar diagnóstico operativo final.',
    },
];

function buildDeterministicNarrative(data, promptMeta) {
    const s = data.summary || {};
    const m = data.market || {};
    const top = (data.mostActiveStrikes || []).slice(0, 8);
    const missing = data.missingInputs || [];
    const pendingModules = (data.moduleStatus || []).filter((mod) => mod.status !== 'ready');
    const promptCoverage = promptMeta.length
        ? `${promptMeta.filter((p) => !p.missing).length}/${promptMeta.length}`
        : '0/0';

    return `## Sesgo del Mercado
${s.pcRatio > 1 ? 'Sesgo defensivo (put-dominant)' : 'Sesgo ofensivo (call-dominant)'} con P/C ratio **${(s.pcRatio ?? 0).toFixed(3)}**.

## Niveles Clave
- **Precio NQ actual:** ${m.nqPrice ?? 'N/D'}
- **Call Wall:** ${s.callWall ?? 'N/D'}
- **Put Wall:** ${s.putWall ?? 'N/D'}
- **Max Pain (NQ):** ${data.maxPain?.nqEquivalent ?? 'N/D'}
- **Strike de atracción:** ${s.attractionStrike ?? 'N/D'}

## Flujo y Actividad
${top.length ? top.map((row) => `- ${row.nqStrike}: Vol ${row.totalVol}, OI C:${row.callOI}/P:${row.putOI}, OIΔ C:${row.callOIChg}/P:${row.putOIChg}`).join('\n') : '- Sin datos de Most Active aún.'}

## Estado de Módulos (Prompt-Driven)
- Prompts cargados: **${promptCoverage}**
- Módulos pendientes: **${pendingModules.length}**
${pendingModules.length ? pendingModules.map((p) => `- ${p.label}`).join('\n') : '- Todos los módulos base tienen insumos.'}

## Riesgos de Datos
${missing.length ? missing.map((item) => `- Falta: ${item}`).join('\n') : '- Sin faltantes críticos detectados.'}

## Diagnóstico Operativo
- Priorizar compras cerca de ${s.putWall ?? 'soporte N/D'} si hay absorción.
- Priorizar ventas cerca de ${s.callWall ?? 'resistencia N/D'} si hay rechazo.
- Evitar operar en rango central sin confirmación de flujo.`;
}

async function runClaudeAnalysis(context, prompts, data) {
    const systemPrompt = `Eres un analista institucional de NQ. Debes respetar el orden modular de prompts y producir un diagnóstico accionable en español con zonas de compra/venta y gestión de riesgo.`;
    const userPrompt = `Prompts (orden real):\n${prompts.map((p) => `Prompt${p.id}:\n${p.text || '[missing]'}`).join('\n\n')}\n\nDatos normalizados actuales:\n${JSON.stringify(data, null, 2)}\n\nGenera el reporte final consolidado para la sesión actual.`;

    const response = await fetch('https://api.anthropic.com/v1/messages', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
            'x-api-key': context.env.CLAUDE_API_KEY,
            'anthropic-version': '2023-06-01',
        },
        body: JSON.stringify({
            model: 'claude-sonnet-4-6',
            max_tokens: 4096,
            system: systemPrompt,
            messages: [{ role: 'user', content: userPrompt }],
        }),
    });

    if (!response.ok) {
        const body = await response.text();
        throw new Error(`Claude API error ${response.status}: ${body}`);
    }

    const payload = await response.json();
    return {
        narrative: payload.content?.[0]?.text || 'No se pudo generar salida de Claude.',
        usage: payload.usage || null,
    };
}

async function saveRun(db, runDate, narrative, data, fromCache) {
    await db.prepare(
        `INSERT INTO analysis_runs (run_date, run_type, from_cache, narrative, data_json, created_at)
         VALUES (?, ?, ?, ?, ?, datetime('now'))
         ON CONFLICT(run_date, run_type)
         DO UPDATE SET from_cache = excluded.from_cache, narrative = excluded.narrative, data_json = excluded.data_json, created_at = datetime('now')`
    ).bind(runDate, RUN_TYPE, fromCache ? 1 : 0, narrative, JSON.stringify(data)).run();
}

async function getCachedRun(db, runDate) {
    return db.prepare(
        `SELECT narrative, data_json, created_at
         FROM analysis_runs
         WHERE run_date = ? AND run_type = ?`
    ).bind(runDate, RUN_TYPE).first();
}

export async function onRequestPost(context) {
    const db = context.env.DB;
    try {
        if (!context.env.CLAUDE_API_KEY) {
            return jsonError('CLAUDE_API_KEY no configurado.', 400);
        }

        const runDate = new Date().toISOString().slice(0, 10);
        const data = await buildDashboardData(db);
        const prompts = PROMPT_BUNDLE.map((prompt) => ({ ...prompt, missing: false }));

        const result = await runClaudeAnalysis(context, prompts, data);

        const payload = {
            ...data,
            prompts: prompts.map((p) => ({ id: p.id, missing: p.missing })),
            claudeUsage: result.usage,
        };

        await saveRun(db, runDate, result.narrative, payload, false);

        return Response.json({
            success: true,
            fromCache: false,
            runDate,
            narrative: result.narrative,
            data: payload,
            createdAt: new Date().toISOString(),
            usage: result.usage,
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}

export async function onRequestGet(context) {
    const db = context.env.DB;
    try {
        const runDate = new Date().toISOString().slice(0, 10);
        const cached = await getCachedRun(db, runDate);
        if (!cached) {
            return Response.json({
                success: false,
                message: 'No hay ejecución de orquestación para hoy. Usa POST /api/analysis.',
            });
        }
        return Response.json({
            success: true,
            fromCache: true,
            runDate,
            narrative: cached.narrative,
            data: JSON.parse(cached.data_json),
            createdAt: cached.created_at,
        });
    } catch (error) {
        return jsonError(error.message, 500);
    }
}
