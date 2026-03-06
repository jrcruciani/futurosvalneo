export async function onRequestPost(context) {
    const db = context.env.DB;

    try {
        await db.batch([
            db.prepare(`CREATE TABLE IF NOT EXISTS source_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                source_name TEXT NOT NULL,
                module_name TEXT NOT NULL,
                file_name TEXT NOT NULL,
                file_path TEXT NOT NULL,
                mime_type TEXT NOT NULL,
                file_size INTEGER NOT NULL DEFAULT 0,
                checksum TEXT NOT NULL DEFAULT '',
                as_of_date TEXT,
                status TEXT NOT NULL DEFAULT 'ingested',
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS option_chain_raw (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of_date TEXT NOT NULL,
                module_name TEXT NOT NULL,
                strike REAL NOT NULL,
                option_side TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                metric_value REAL NOT NULL,
                source_file_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(as_of_date, module_name, strike, option_side, metric_name)
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS market_ticks (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                tick_time TEXT NOT NULL UNIQUE,
                nq_price REAL NOT NULL,
                volume REAL NOT NULL DEFAULT 0,
                source_file_id INTEGER,
                metadata_json TEXT NOT NULL DEFAULT '{}',
                created_at TEXT DEFAULT (datetime('now'))
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS volatility_signals (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of_date TEXT NOT NULL,
                signal_type TEXT NOT NULL,
                level REAL NOT NULL DEFAULT -1,
                value REAL NOT NULL DEFAULT 0,
                notes TEXT NOT NULL DEFAULT '',
                source_file_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(as_of_date, signal_type, level)
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS darkpool_levels (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                as_of_date TEXT NOT NULL,
                qqq_level REAL NOT NULL,
                nq_level REAL NOT NULL,
                conversion_ratio REAL NOT NULL,
                volume REAL NOT NULL DEFAULT 0,
                side TEXT NOT NULL DEFAULT '',
                source_file_id INTEGER,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(as_of_date, qqq_level, side)
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS analysis_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                run_date TEXT NOT NULL,
                run_type TEXT NOT NULL,
                from_cache INTEGER NOT NULL DEFAULT 0,
                narrative TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now')),
                UNIQUE(run_date, run_type)
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS market_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                snapshot_date TEXT NOT NULL UNIQUE,
                snapshot_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )`),
            db.prepare(`CREATE TABLE IF NOT EXISTS data_cache (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                cache_key TEXT NOT NULL UNIQUE,
                session_id TEXT NOT NULL,
                data_json TEXT NOT NULL,
                created_at TEXT DEFAULT (datetime('now'))
            )`),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_source_module_date ON source_files(module_name, as_of_date, created_at)`),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_option_module_metric_date ON option_chain_raw(module_name, metric_name, as_of_date)`),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_ticks_time ON market_ticks(tick_time)`),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_darkpool_date ON darkpool_levels(as_of_date)`),
            db.prepare(`CREATE INDEX IF NOT EXISTS idx_analysis_date_type ON analysis_runs(run_date, run_type)`)
        ]);

        return Response.json({ success: true, message: 'Base de datos inicializada (FuturesValNeo).' });
    } catch (error) {
        return Response.json({ error: error.message }, { status: 500 });
    }
}
