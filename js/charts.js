// FuturesVal Charts (Chart.js)
const Charts = {
    oiChart: null,
    gexChart: null,

    // Color palette
    colors: {
        callGreen: 'rgba(74, 222, 128, 0.8)',
        callGreenBg: 'rgba(74, 222, 128, 0.3)',
        putRed: 'rgba(248, 113, 113, 0.8)',
        putRedBg: 'rgba(248, 113, 113, 0.3)',
        maxPainGold: 'rgba(251, 191, 36, 1)',
        gexPositive: 'rgba(96, 165, 250, 0.8)',
        gexNegative: 'rgba(251, 146, 60, 0.8)',
        gridLine: 'rgba(0, 0, 0, 0.06)',
        textMuted: '#6b7280'
    },

    // Shared chart defaults
    defaults() {
        Chart.defaults.font.family = "'Inter', system-ui, sans-serif";
        Chart.defaults.font.size = 11;
        Chart.defaults.color = this.colors.textMuted;
    },

    // OI Distribution chart
    renderOI(canvasId, data, maxPainNQ) {
        const ctx = document.getElementById(canvasId)?.getContext('2d');
        if (!ctx) return;

        if (this.oiChart) this.oiChart.destroy();

        const labels = data.nqStrikes.map(s => s.toLocaleString());

        this.oiChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [
                    {
                        label: 'Call OI',
                        data: data.callOI,
                        backgroundColor: this.colors.callGreen,
                        borderRadius: 2,
                        barPercentage: 0.9,
                        categoryPercentage: 0.8
                    },
                    {
                        label: 'Put OI',
                        data: data.putOI.map(v => -v),
                        backgroundColor: this.colors.putRed,
                        borderRadius: 2,
                        barPercentage: 0.9,
                        categoryPercentage: 0.8
                    }
                ]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: { mode: 'index', intersect: false },
                plugins: {
                    legend: { display: true, position: 'top', labels: { boxWidth: 12, padding: 16 } },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => {
                                const val = Math.abs(ctx.raw);
                                return `${ctx.dataset.label}: ${val.toLocaleString()}`;
                            }
                        }
                    },
                    annotation: maxPainNQ ? {
                        annotations: {
                            maxPainLine: {
                                type: 'line',
                                xMin: labels.indexOf(maxPainNQ.toLocaleString()),
                                xMax: labels.indexOf(maxPainNQ.toLocaleString()),
                                borderColor: this.colors.maxPainGold,
                                borderWidth: 2,
                                borderDash: [6, 3],
                                label: {
                                    content: `Max Pain: ${maxPainNQ.toLocaleString()}`,
                                    enabled: true,
                                    position: 'start',
                                    backgroundColor: this.colors.maxPainGold,
                                    color: '#1e1b2e',
                                    font: { size: 10, weight: 'bold' }
                                }
                            }
                        }
                    } : {}
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: {
                            maxRotation: 45,
                            autoSkip: true,
                            maxTicksLimit: 20,
                            font: { size: 9 }
                        }
                    },
                    y: {
                        grid: { color: this.colors.gridLine },
                        ticks: {
                            callback: (v) => {
                                const abs = Math.abs(v);
                                return abs >= 1000 ? (abs / 1000).toFixed(0) + 'K' : abs;
                            }
                        }
                    }
                }
            }
        });
    },

    // Gamma Exposure chart
    renderGEX(canvasId, greeksData) {
        const ctx = document.getElementById(canvasId)?.getContext('2d');
        if (!ctx) return;

        if (this.gexChart) this.gexChart.destroy();

        // Filter to strikes with non-zero GEX
        const filtered = greeksData.filter(g => Math.abs(g.netGEX) > 10);
        const labels = filtered.map(g => g.nqStrike.toLocaleString());
        const values = filtered.map(g => g.netGEX);
        const bgColors = values.map(v => v >= 0 ? this.colors.gexPositive : this.colors.gexNegative);

        this.gexChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: [{
                    label: 'Net GEX',
                    data: values,
                    backgroundColor: bgColors,
                    borderRadius: 2
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: (ctx) => `Net GEX: ${ctx.raw.toLocaleString()}`
                        }
                    }
                },
                scales: {
                    x: {
                        grid: { display: false },
                        ticks: { maxRotation: 45, autoSkip: true, maxTicksLimit: 20, font: { size: 9 } }
                    },
                    y: {
                        grid: { color: this.colors.gridLine },
                        ticks: {
                            callback: (v) => {
                                if (Math.abs(v) >= 1000000) return (v / 1000000).toFixed(1) + 'M';
                                if (Math.abs(v) >= 1000) return (v / 1000).toFixed(0) + 'K';
                                return v;
                            }
                        }
                    }
                }
            }
        });
    }
};
