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
        document.getElementById('btn-init-db')?.addEventListener('click', () => this.initDB());
        document.getElementById('btn-refresh')?.addEventListener('click', () => this.loadData(true));
        document.getElementById('btn-snapshot')?.addEventListener('click', () => this.saveSnapshot());
        document.getElementById('btn-analysis')?.addEventListener('click', () => this.runAnalysis(false));
        document.getElementById('btn-analysis-force')?.addEventListener('click', () => this.runAnalysis(true));

        document.getElementById('form-cme')?.addEventListener('submit', (e) => this.onSubmitCME(e));
        document.getElementById('form-darkpool')?.addEventListener('submit', (e) => this.onSubmitDarkpool(e));
        document.getElementById('form-ninja')?.addEventListener('submit', (e) => this.onSubmitNinja(e));
        document.getElementById('form-quikvol')?.addEventListener('submit', (e) => this.onSubmitQuikvol(e));
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

    async initDB() {
        try {
            await API.init();
            this.showToast('Base de datos inicializada.');
        } catch (error) {
            this.showError(`Init DB: ${error.message}`);
        }
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
            const useClaude = !!document.getElementById('use-claude')?.checked;
            this.analysis = await API.runAnalysis(force, useClaude);
            this.renderAnalysis();
            this.showToast(this.analysis.fromCache ? 'Análisis cacheado.' : 'Análisis generado.');
        } catch (error) {
            this.showError(`Análisis: ${error.message}`);
        } finally {
            this.setLoading('loading-analysis', false);
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

    async onSubmitCME(event) {
        event.preventDefault();
        const moduleName = document.getElementById('cme-module')?.value;
        const fileInput = document.getElementById('cme-file');
        const asOfDate = document.getElementById('cme-date')?.value || null;
        const file = fileInput?.files?.[0];
        if (!moduleName || !file) {
            this.showError('Selecciona módulo CME y archivo.');
            return;
        }

        try {
            const isBinary = file.type.startsWith('image/') || file.type === 'application/pdf';
            const content = isBinary ? await this.fileToBase64(file) : await file.text();
            const result = await API.ingestCME(moduleName, file.name, content, asOfDate, file.type || 'text/csv');
            this.showToast(`CME ${moduleName}: ${result.metricsInserted} métricas.`);
            fileInput.value = '';
            await this.loadData(true);
        } catch (error) {
            this.showError(`Ingesta CME: ${error.message}`);
        }
    },

    async onSubmitDarkpool(event) {
        event.preventDefault();
        const fileInput = document.getElementById('darkpool-file');
        const ratioInput = document.getElementById('darkpool-ratio');
        const asOfDate = document.getElementById('darkpool-date')?.value || null;
        const file = fileInput?.files?.[0];
        const conversionRatio = Number(ratioInput?.value);
        if (!file) {
            this.showError('Selecciona el CSV de DarkPool.');
            return;
        }
        if (!Number.isFinite(conversionRatio) || conversionRatio <= 0) {
            this.showError('Define un conversion ratio válido (QQQ -> NQ).');
            return;
        }

        try {
            const content = await file.text();
            const result = await API.ingestDarkpool(file.name, content, conversionRatio, asOfDate);
            this.showToast(`DarkPool: ${result.levelsInserted} niveles.`);
            fileInput.value = '';
            await this.loadData(true);
        } catch (error) {
            this.showError(`Ingesta DarkPool: ${error.message}`);
        }
    },

    async onSubmitNinja(event) {
        event.preventDefault();
        const fileInput = document.getElementById('ninja-file');
        const nqPrice = Number(document.getElementById('ninja-price')?.value);
        const volume = Number(document.getElementById('ninja-volume')?.value || 0);
        const tickTime = document.getElementById('ninja-time')?.value;
        const file = fileInput?.files?.[0];

        try {
            let result;
            if (file) {
                const content = await file.text();
                result = await API.ingestNinja({
                    fileName: file.name,
                    content,
                    volume,
                    tickTime: tickTime || new Date().toISOString(),
                });
                fileInput.value = '';
            } else {
                if (!Number.isFinite(nqPrice) || nqPrice <= 0) {
                    this.showError('NQ price inválido.');
                    return;
                }
                result = await API.ingestNinja({
                    nqPrice,
                    volume,
                    tickTime: tickTime || new Date().toISOString(),
                });
            }
            this.showToast(`Tick Ninja guardado (${result.ticksInserted}).`);
            await this.loadData(true);
        } catch (error) {
            this.showError(`Ingesta Ninja: ${error.message}`);
        }
    },

    async onSubmitQuikvol(event) {
        event.preventDefault();
        const fileInput = document.getElementById('quikvol-file');
        const notes = document.getElementById('quikvol-notes')?.value || '';
        const file = fileInput?.files?.[0];
        if (!file) {
            this.showError('Selecciona archivo QuikVol (imagen o JSON).');
            return;
        }

        try {
            const content = file.type.startsWith('image/')
                ? await this.fileToBase64(file)
                : await file.text();
            const result = await API.ingestQuikvol({
                fileName: file.name,
                content,
                mimeType: file.type || 'application/octet-stream',
                signals: notes
                    ? [{ signalType: 'quikvol_manual_note', level: -1, value: 1, notes }]
                    : [],
            });
            this.showToast(`QuikVol cargado (${result.signalsInserted} señales).`);
            fileInput.value = '';
            await this.loadData(true);
        } catch (error) {
            this.showError(`Ingesta QuikVol: ${error.message}`);
        }
    },

    fileToBase64(file) {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            reader.onload = () => resolve(String(reader.result || ''));
            reader.onerror = () => reject(new Error('No se pudo leer el archivo binario.'));
            reader.readAsDataURL(file);
        });
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
        this.setText('metric-oi', `C:${s.totalCallOI ?? 0} / P:${s.totalPutOI ?? 0}`);
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
                    <span>${mod.label}</span>
                    <span class="module-meta">${mod.fileCount} archivos · ${mod.lastIngested ? new Date(mod.lastIngested).toLocaleString() : 'sin carga'}</span>
                </li>
            `).join('')
            : '<li class="module-item pending">Sin estado de módulos.</li>';
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
