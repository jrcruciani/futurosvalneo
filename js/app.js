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
            document.getElementById('status-panel')?.classList.toggle('hidden');
        });
        document.addEventListener('click', () => {
            document.getElementById('status-panel')?.classList.add('hidden');
        });
        document.getElementById('btn-sync-r2')?.addEventListener('click', () => this.syncDatasources());
        document.getElementById('btn-refresh')?.addEventListener('click', () => this.loadData(true));
        document.getElementById('btn-snapshot')?.addEventListener('click', () => this.saveSnapshot());
        document.getElementById('btn-analysis')?.addEventListener('click', () => this.runAnalysis(false));
        document.getElementById('btn-analysis-force')?.addEventListener('click', () => this.runAnalysis(true));

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

    async runAnalysis(force = false) {
        if (!this.data) {
            this.showError('Primero carga datos del dashboard.');
            return;
        }

        this.setLoading('loading-analysis', true);
        try {
            this.analysis = await API.runAnalysis(force, true);
            this.renderAnalysis();
            this.showToast(this.analysis.fromCache ? 'Análisis cacheado.' : 'Análisis generado.');
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

        const missing = document.getElementById('missing-inputs');
        if (missing) {
            missing.innerHTML = (d.missingInputs || []).length
                ? d.missingInputs.map((x) => `<li>${x}</li>`).join('')
                : '<li>Sin faltantes críticos.</li>';
        }

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
                <span class="market-label">Último Tick</span>
                <span class="market-value">${m.lastTickTime ? new Date(m.lastTickTime).toLocaleTimeString() : 'N/D'}</span>
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

        const btn = document.getElementById('btn-status');
        if (btn) {
            const allReady = modules.length > 0 && modules.every((m) => m.status === 'ready');
            btn.className = `btn btn-ghost btn-sm ${allReady ? 'all-ready' : 'has-pending'}`;
        }
    },

    renderAnalysis() {
        const el = document.getElementById('analysis-content');
        if (!el || !this.analysis) return;
        el.classList.remove('empty');
        el.innerHTML = this.md(this.analysis.narrative || 'Sin narrativa.');
        this.setText('analysis-time', this.analysis.createdAt ? `Generado: ${new Date(this.analysis.createdAt).toLocaleString()}` : '');
        const badge = document.getElementById('analysis-cache');
        if (badge) {
            badge.textContent = this.analysis.fromCache ? 'Cached' : 'Fresh';
            badge.className = `badge ${this.analysis.fromCache ? 'cached' : 'fresh'}`;
        }
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
            .replace(/\n- /g, '\n• ')
            .replace(/\n/g, '<br>');
    },
};

document.addEventListener('DOMContentLoaded', () => App.init());
