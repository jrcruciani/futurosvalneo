const App = {
    data: null,
    analysis: null,

    async init() {
        this.bindEvents();
        await this.loadData();
        await this.loadHistory();
        await this.loadLatestAnalysis();
    },

    bindEvents() {
        document.getElementById('btn-status')?.addEventListener('click', (e) => {
            e.stopPropagation();
            const panel = document.getElementById('status-panel');
            panel?.classList.toggle('hidden');
            if (panel && !panel.classList.contains('hidden')) {
                this.loadOpsStatus();
            }
        });
        document.getElementById('status-panel')?.addEventListener('click', (e) => {
            e.stopPropagation();
        });
        document.addEventListener('click', () => {
            document.getElementById('status-panel')?.classList.add('hidden');
        });
        document.getElementById('btn-sync-r2')?.addEventListener('click', () => this.syncDatasources());
        document.getElementById('btn-refresh')?.addEventListener('click', () => this.loadData(true));
        document.getElementById('btn-snapshot')?.addEventListener('click', () => this.saveSnapshot());
        document.getElementById('btn-analysis')?.addEventListener('click', () => this.runAnalysis());

    },

    showError(message) {
        const el = document.getElementById('error-banner');
        if (!el) return;
        el.textContent = message;
        el.classList.remove('hidden');
        setTimeout(() => el.classList.add('hidden'), 8000);
    },

    showToast(message) {
        const toast = document.createElement('div');
        toast.className = 'toast';
        toast.textContent = message;
        document.body.appendChild(toast);
        setTimeout(() => toast.remove(), 3200);
    },

    setLoading(id, active) {
        const el = document.getElementById(id);
        if (!el) return;
        el.classList.toggle('hidden', !active);
    },

    async loadData(force = false) {
        this.setLoading('loading-main', true);
        try {
            this.data = await API.getOptionsData(force);
            this.renderDashboard();
        } catch (error) {
            this.showError(`Carga de datos: ${error.message}`);
        } finally {
            this.setLoading('loading-main', false);
        }
    },

    async loadHistory() {
        try {
            const history = await API.getHistory(7);
            this.renderHistory(history.snapshots || []);
        } catch (error) {
            this.showError(`Histórico: ${error.message}`);
        }
    },

    async loadLatestAnalysis() {
        try {
            const response = await API.getAnalysis();
            if (response.success) {
                this.analysis = response;
                this.renderAnalysis();
            }
        } catch (_) {
            // No analysis yet; keep empty state.
        }
    },

    async runAnalysis() {
        if (!this.data) {
            this.showError('Primero carga datos del dashboard.');
            return;
        }

        this.setLoading('loading-analysis', true);
        try {
            this.analysis = await API.runAnalysis();
            this.renderAnalysis();
            this.showToast('Análisis generado.');
        } catch (error) {
            this.showError(`Análisis: ${error.message}`);
        } finally {
            this.setLoading('loading-analysis', false);
        }
    },

    async syncDatasources() {
        this.setLoading('loading-main', true);
        try {
            const result = await API.syncDatasources(true);
            const s = result.summary || {};
            this.showToast(`Sync R2: ${s.ok} procesados, ${s.skipped} omitidos${s.errors ? `, ${s.errors} errores` : ''}.`);
            if (s.errors) {
                const failed = result.results.filter((r) => r.status === 'error').map((r) => r.reason).join('; ');
                this.showError(`Errores en Sync R2: ${failed}`);
            }
            await this.loadData(true);
        } catch (error) {
            this.showError(`Sync R2: ${error.message}`);
        } finally {
            this.setLoading('loading-main', false);
        }
    },

    async loadOpsStatus() {
        try {
            const status = await API.getOpsStatus();
            this.renderModuleStatus({ moduleStatus: status.moduleStatus, missingInputs: status.pendingModules });
        } catch (error) {
            const list = document.getElementById('module-status');
            if (list) list.innerHTML = '<li class="module-item pending"><span>Error cargando estado</span></li>';
        }
    },

    async saveSnapshot() {
        try {
            const result = await API.saveSnapshot();
            this.showToast(`Snapshot ${result.snapshotDate} guardado.`);
            await this.loadHistory();
        } catch (error) {
            this.showError(`Snapshot: ${error.message}`);
        }
    },

    renderDashboard() {
        const d = this.data;
        if (!d) return;

        this.renderMarketBar(d);
        this.renderMetrics(d);
        this.renderMostActive(d);
        this.renderModuleStatus(d);

        const ts = document.getElementById('data-timestamp');
        if (ts) ts.textContent = new Date(d.timestamp).toLocaleString();
        const cache = document.getElementById('cache-badge');
        if (cache) {
            cache.textContent = d._fromCache ? 'Cached' : 'Fresh';
            cache.className = `badge ${d._fromCache ? 'cached' : 'fresh'}`;
        }
    },

    renderMarketBar(d) {
        const m = d.market || {};
        const el = document.getElementById('market-bar');
        if (!el) return;
        el.innerHTML = `
            <div class="market-item">
                <span class="market-label">NQ</span>
                <span class="market-value">${m.nqPrice ?? 'N/D'}</span>
            </div>
            <div class="market-item">
                <span class="market-label">QQQ (calc)</span>
                <span class="market-value">${m.qqqPrice ?? 'N/D'}</span>
            </div>
            <div class="market-item">
                <span class="market-label">Ratio</span>
                <span class="market-value">${m.conversionRatio ?? 'N/D'}x</span>
            </div>
            <div class="market-item">
                <span class="market-label">Madrid</span>
                <span class="market-value">${m.lastTickTime ? new Date(m.lastTickTime).toLocaleTimeString('es-ES', { timeZone: 'Europe/Madrid', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : 'N/D'}</span>
            </div>
            <div class="market-item">
                <span class="market-label">Colombia</span>
                <span class="market-value">${m.lastTickTime ? new Date(m.lastTickTime).toLocaleTimeString('es-CO', { timeZone: 'America/Bogota', hour: '2-digit', minute: '2-digit', second: '2-digit' }) : 'N/D'}</span>
            </div>
        `;
    },

    renderMetrics(d) {
        const s = d.summary || {};
        this.setText('metric-max-pain', d.maxPain?.nqEquivalent ?? 'N/D');
        this.setText('metric-call-wall', s.callWall ?? 'N/D');
        this.setText('metric-put-wall', s.putWall ?? 'N/D');
        this.setText('metric-pcr', s.pcRatio != null ? s.pcRatio.toFixed(3) : 'N/D');
        this.setText('metric-vol', `C:${s.totalCallVolume ?? 0} / P:${s.totalPutVolume ?? 0}`);
    },

    renderMostActive(d) {
        const body = document.getElementById('active-strikes-body');
        if (!body) return;
        const rows = d.mostActiveStrikes || [];
        if (!rows.length) {
            body.innerHTML = '<tr><td colspan="8" class="empty-cell">Sin datos aún.</td></tr>';
            return;
        }

        body.innerHTML = rows.map((row) => `
            <tr>
                <td class="strike">${row.nqStrike}</td>
                <td class="num">${row.totalVol}</td>
                <td class="num call">${row.callVol}</td>
                <td class="num put">${row.putVol}</td>
                <td class="num">${row.callOI}</td>
                <td class="num">${row.putOI}</td>
                <td class="num">${row.callOIChg > 0 ? '+' : ''}${row.callOIChg}</td>
                <td class="num">${row.putOIChg > 0 ? '+' : ''}${row.putOIChg}</td>
            </tr>
        `).join('');
    },

    renderModuleStatus(d) {
        const list = document.getElementById('module-status');
        if (!list) return;
        const modules = d.moduleStatus || [];
        list.innerHTML = modules.length
            ? modules.map((mod) => `
                <li class="module-item ${mod.status}">
                    <span>${mod.status === 'ready' ? '✓' : '○'} ${mod.label}</span>
                    <span class="module-meta">${mod.fileCount} archivos · ${mod.lastIngested ? new Date(mod.lastIngested).toLocaleString() : 'sin carga'}</span>
                </li>
            `).join('')
            : '<li class="module-item pending">Sin estado de módulos.</li>';

        const missing = document.getElementById('missing-inputs');
        if (missing) {
            const items = d.missingInputs || [];
            missing.innerHTML = items.length
                ? items.map((x) => `<li class="module-item pending"><span>○ ${x}</span></li>`).join('')
                : '<li class="module-item ready"><span>✓ Sin faltantes críticos</span></li>';
        }

        const btn = document.getElementById('btn-status');
        if (btn) {
            const allReady = modules.length > 0 && modules.every((m) => m.status === 'ready');
            btn.className = `btn btn-ghost btn-sm ${allReady ? 'all-ready' : 'has-pending'}`;
        }
    },

    renderAnalysis() {
        const el = document.getElementById('analysis-content');
        if (!el || !this.analysis) return;

        const d = this.analysis.data || this.data || {};
        const s = d.summary || {};
        const m = d.market || {};
        const visuals = document.getElementById('analysis-visuals');

        // Show visuals if we have data
        if (visuals && (s.callWall || s.putWall)) {
            visuals.classList.remove('hidden');
            this.renderAnalysisMetrics(d);
            this.renderLevelsBar(d);
            this.renderAnalysisCharts(d);
        }

        // Render narrative
        el.classList.remove('empty');
        el.innerHTML = this.md(this.analysis.narrative || 'Sin narrativa.');

        this.setText('analysis-time', this.analysis.createdAt ? `Generado: ${new Date(this.analysis.createdAt).toLocaleString()}` : '');
        const badge = document.getElementById('analysis-cache');
        if (badge) {
            badge.textContent = this.analysis.fromCache ? 'Cached' : 'Fresh';
            badge.className = `badge ${this.analysis.fromCache ? 'cached' : 'fresh'}`;
        }
    },

    renderAnalysisMetrics(d) {
        const s = d.summary || {};
        const m = d.market || {};
        const em = d.expectedMove || {};

        // Bias
        const biasEl = document.getElementById('analysis-bias');
        if (biasEl) {
            const pcr = s.pcRatio || 0;
            const isBullish = pcr < 0.8;
            const isBearish = pcr > 1.2;
            biasEl.textContent = isBearish ? 'Bearish (P/C ' + pcr.toFixed(2) + ')' :
                                 isBullish ? 'Bullish (P/C ' + pcr.toFixed(2) + ')' :
                                 'Neutral (P/C ' + pcr.toFixed(2) + ')';
            biasEl.className = 'analysis-metric-value ' + (isBearish ? 'bearish' : isBullish ? 'bullish' : 'neutral');
        }

        // Expected Move
        const moveEl = document.getElementById('analysis-move');
        if (moveEl && em.movePercent) {
            moveEl.textContent = em.movePercent.toFixed(1) + '%';
            moveEl.className = 'analysis-metric-value';
        }

        // Volume ratio
        const volEl = document.getElementById('analysis-vol-ratio');
        if (volEl) {
            const cv = s.totalCallVolume || 0;
            const pv = s.totalPutVolume || 0;
            volEl.textContent = cv.toLocaleString() + ' / ' + pv.toLocaleString();
            volEl.className = 'analysis-metric-value ' + (cv > pv ? 'bullish' : cv < pv ? 'bearish' : 'neutral');
        }

        // Net OI Change
        const netEl = document.getElementById('analysis-net-oi');
        if (netEl) {
            const net = s.netOIChange || 0;
            netEl.textContent = (net > 0 ? '+' : '') + net.toLocaleString();
            netEl.className = 'analysis-metric-value ' + (net > 0 ? 'bullish' : net < 0 ? 'bearish' : 'neutral');
        }
    },

    renderLevelsBar(d) {
        const bar = document.getElementById('levels-bar');
        const legend = document.getElementById('levels-legend');
        if (!bar) return;

        const s = d.summary || {};
        const m = d.market || {};
        const mp = d.maxPain?.nqEquivalent;
        const nq = m.nqPrice;
        const cw = s.callWall;
        const pw = s.putWall;
        const attr = s.attractionStrike;

        const levels = [
            { label: 'Put Wall', value: pw, color: '#f87171' },
            { label: 'Max Pain', value: mp, color: '#fbbf24' },
            { label: 'Attraction', value: attr, color: '#a78bfa' },
            { label: 'NQ Price', value: nq, color: '#60a5fa' },
            { label: 'Call Wall', value: cw, color: '#4ade80' },
        ].filter(l => l.value != null);

        if (levels.length < 2) {
            bar.innerHTML = '<span style="color:var(--text-muted);font-size:0.8rem;display:flex;align-items:center;justify-content:center;height:100%">Sin datos suficientes</span>';
            return;
        }

        const values = levels.map(l => l.value);
        const min = Math.min(...values);
        const max = Math.max(...values);
        const range = max - min || 1;
        const pad = range * 0.08;

        bar.innerHTML = levels.map(l => {
            const pct = ((l.value - min + pad) / (range + pad * 2)) * 100;
            return `<div class="level-marker" style="left:${pct}%">
                <span class="level-price" style="color:${l.color}">${Math.round(l.value).toLocaleString()}</span>
                <span class="level-dot" style="background:${l.color}"></span>
                <span class="level-label">${l.label}</span>
            </div>`;
        }).join('');

        if (legend) {
            legend.innerHTML = levels.map(l =>
                `<span class="levels-legend-item">
                    <span class="levels-legend-dot" style="background:${l.color}"></span>
                    ${l.label}: ${Math.round(l.value).toLocaleString()}
                </span>`
            ).join('');
        }
    },

    renderAnalysisCharts(d) {
        const oi = d.oiDistribution;
        if (oi && oi.nqStrikes?.length && typeof Chart !== 'undefined') {
            Charts.defaults();
            Charts.renderOI('chart-oi-dist', oi, d.maxPain?.nqEquivalent);
            this.renderVolChart(oi);
        }
    },

    renderVolChart(oi) {
        const ctx = document.getElementById('chart-vol-cp')?.getContext('2d');
        if (!ctx) return;

        if (this._volChart) this._volChart.destroy();

        // Aggregate top strikes by volume for a cleaner chart
        const indices = oi.nqStrikes.map((s, i) => i)
            .filter(i => (oi.callVolume[i] || 0) + (oi.putVolume[i] || 0) > 0)
            .sort((a, b) => (oi.callVolume[b] + oi.putVolume[b]) - (oi.callVolume[a] + oi.putVolume[a]))
            .slice(0, 15)
            .sort((a, b) => oi.nqStrikes[a] - oi.nqStrikes[b]);

        if (!indices.length) return;

        const labels = indices.map(i => oi.nqStrikes[i].toLocaleString());
        const callData = indices.map(i => oi.callVolume[i] || 0);
        const putData = indices.map(i => -(oi.putVolume[i] || 0));

        this._volChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    { label: 'Call Vol', data: callData, backgroundColor: 'rgba(74, 222, 128, 0.7)', borderRadius: 2 },
                    { label: 'Put Vol', data: putData, backgroundColor: 'rgba(248, 113, 113, 0.7)', borderRadius: 2 },
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: true, position: 'top', labels: { boxWidth: 10, padding: 12, font: { size: 10 } } },
                    tooltip: { callbacks: { label: (c) => `${c.dataset.label}: ${Math.abs(c.raw).toLocaleString()}` } }
                },
                scales: {
                    x: { grid: { display: false }, ticks: { maxRotation: 45, autoSkip: true, font: { size: 9 } } },
                    y: { grid: { color: 'rgba(0,0,0,0.06)' }, ticks: { callback: v => { const a = Math.abs(v); return a >= 1000 ? (a/1000).toFixed(0)+'K' : a; } } }
                }
            }
        });
    },

    renderHistory(snapshots) {
        const body = document.getElementById('history-body');
        if (!body) return;
        if (!snapshots.length) {
            body.innerHTML = '<tr><td colspan="5" class="empty-cell">Sin snapshots recientes.</td></tr>';
            return;
        }

        body.innerHTML = snapshots.map((snap) => {
            const summary = snap.data?.summary || {};
            return `
                <tr>
                    <td>${snap.snapshotDate}</td>
                    <td>${summary.callWall ?? 'N/D'}</td>
                    <td>${summary.putWall ?? 'N/D'}</td>
                    <td>${summary.pcRatio != null ? Number(summary.pcRatio).toFixed(3) : 'N/D'}</td>
                    <td>${summary.attractionStrike ?? 'N/D'}</td>
                </tr>
            `;
        }).join('');
    },

    setText(id, value) {
        const el = document.getElementById(id);
        if (el) el.textContent = value;
    },

    md(text) {
        return String(text || '')
            .replace(/^### (.+)$/gm, '<h4>$1</h4>')
            .replace(/^## (.+)$/gm, '<h3>$1</h3>')
            .replace(/^# (.+)$/gm, '<h2>$1</h2>')
            .replace(/\*\*(.+?)\*\*/g, '<strong>$1</strong>')
            .replace(/`([^`]+)`/g, '<code>$1</code>')
            .replace(/^- (.+)$/gm, '<li>$1</li>')
            .replace(/((?:<li>.*<\/li>\n?)+)/g, '<ul>$1</ul>')
            .replace(/\n{2,}/g, '</p><p>')
            .replace(/\n/g, '<br>');
    },
};

document.addEventListener('DOMContentLoaded', () => App.init());
