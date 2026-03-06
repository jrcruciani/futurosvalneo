import { buildDashboardData, getPromptBundle, jsonError } from './_helpers.js';

const RUN_TYPE = 'prompt_orchestrator';

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
        const body = await context.request.json();
        const force = !!body.force;
        const useClaude = !!body.useClaude;
        const runDate = new Date().toISOString().slice(0, 10);

        if (!force) {
            const cached = await getCachedRun(db, runDate);
            if (cached) {
                return Response.json({
                    success: true,
                    fromCache: true,
                    runDate,
                    narrative: cached.narrative,
                    data: JSON.parse(cached.data_json),
                    createdAt: cached.created_at,
                });
            }
        }

        const [data, prompts] = await Promise.all([
            buildDashboardData(db),
            getPromptBundle(context.request.url, 11),
        ]);

        let narrative;
        let usage = null;

        if (useClaude) {
            if (!context.env.CLAUDE_API_KEY) {
                return jsonError('CLAUDE_API_KEY no configurado para useClaude=true.', 400);
            }
            const result = await runClaudeAnalysis(context, prompts, data);
            narrative = result.narrative;
            usage = result.usage;
        } else {
            narrative = buildDeterministicNarrative(data, prompts);
        }

        const payload = {
            ...data,
            prompts: prompts.map((p) => ({ id: p.id, missing: p.missing })),
            claudeUsage: usage,
        };

        await saveRun(db, runDate, narrative, payload, false);

        return Response.json({
            success: true,
            fromCache: false,
            runDate,
            narrative,
            data: payload,
            createdAt: new Date().toISOString(),
            usage,
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
