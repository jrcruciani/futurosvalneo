const API = {
    baseUrl: '',

    async request(path, options = {}) {
        const headers = options.body ? { 'Content-Type': 'application/json' } : {};
        const res = await fetch(`${this.baseUrl}${path}`, {
            ...options,
            headers: {
                ...headers,
                ...(options.headers || {}),
            },
        });

        const data = await res.json().catch(() => ({}));
        if (!res.ok) throw new Error(data.error || `HTTP ${res.status}`);
        return data;
    },

    init() {
        return this.request('/api/init', { method: 'POST' });
    },

    getOptionsData(force = false) {
        const qs = force ? '?force=true' : '';
        return this.request(`/api/options-data${qs}`);
    },

    saveSnapshot() {
        return this.request('/api/snapshot', { method: 'POST' });
    },

    getHistory(days = 7) {
        return this.request(`/api/history?days=${days}`);
    },

    runAnalysis(force = false, useClaude = false) {
        return this.request('/api/analysis', {
            method: 'POST',
            body: JSON.stringify({ force, useClaude }),
        });
    },

    getAnalysis() {
        return this.request('/api/analysis');
    },

    syncDatasources(force = false) {
        return this.request('/api/ingest/datasources', {
            method: 'POST',
            body: JSON.stringify({ force }),
        });
    },

    getDatasources() {
        return this.request('/api/ingest/datasources');
    },

    getOpsStatus() {
        return this.request('/api/ops-status');
    },
};
