// ==========================================
// 1. 核心数学与矩阵推演引擎 (严禁修改，保留原样)
// ==========================================

function alignAndResampleTo4H(all_klines_map, symbols, offset_ms = 0) {
    let chunkMap = new Map();
    const FOUR_HOURS_MS = 4 * 3600 * 1000;
    let minTime = Infinity;
    let maxTime = 0;

    for (const sym of symbols) {
        if (!all_klines_map[sym]) continue;
        for (const k of all_klines_map[sym]) {
            let ts_4h = Math.floor((k[0] - offset_ms) / FOUR_HOURS_MS) * FOUR_HOURS_MS + offset_ms;

            if (ts_4h < minTime) minTime = ts_4h;
            if (ts_4h > maxTime) maxTime = ts_4h;

            if (!chunkMap.has(ts_4h)) chunkMap.set(ts_4h, { time: ts_4h });
            let chunk = chunkMap.get(ts_4h);

            if (!chunk[sym]) {
                chunk[sym] = { o: k[4], h: k[4], l: k[4], c: k[4] };
            } else {
                chunk[sym].h = Math.max(chunk[sym].h, k[4]);
                chunk[sym].l = Math.min(chunk[sym].l, k[4]);
                chunk[sym].c = k[4];
            }
        }
    }

    let sortedChunks = [];
    if (minTime !== Infinity) {
        for (let t = minTime; t <= maxTime; t += FOUR_HOURS_MS) {
            if (chunkMap.has(t)) {
                sortedChunks.push(chunkMap.get(t));
            } else {
                sortedChunks.push({ time: t });
            }
        }
    }

    for (let i = 1; i < sortedChunks.length; i++) {
        let prev = sortedChunks[i - 1];
        let curr = sortedChunks[i];
        for (const sym of symbols) {
            if (!curr[sym] && prev[sym]) {
                curr[sym] = { o: prev[sym].c, h: prev[sym].c, l: prev[sym].c, c: prev[sym].c };
            }
        }
    }
    return sortedChunks;
}

function generateHistoricalTradeLogs(df_4h, params, symbols) {
    let trade_logs = [];
    let positions = {};
    for (let sym of symbols) positions[sym] = { qty: 0 };

    let min_warmup = Math.max(params.MOM_WINDOW, params.VOL_WINDOW, params.BTC_TREND_WINDOW);
    let trade_mode = params.TRADE_MODE || 'LONG_ONLY';

    for (let i = 0; i < df_4h.length; i++) {
        let row = df_4h[i];
        row.TR = {}; row.ATR = {}; row.VOL = {}; row.MOM = {}; row.ADJ_MOM = {};

        if (i >= params.BTC_TREND_WINDOW - 1 && row['BTCUSDT']) {
            let sum = 0;
            for (let j = 0; j < params.BTC_TREND_WINDOW; j++) sum += df_4h[i - j]['BTCUSDT'].c;
            row.BTC_SMA = sum / params.BTC_TREND_WINDOW;
            row.BTC_TREND_ON = row['BTCUSDT'].c > row.BTC_SMA;
        } else {
            row.BTC_TREND_ON = false;
        }

        for (const sym of symbols) {
            if (!row[sym]) continue;
            let c = row[sym].c;
            let h = row[sym].h;
            let l = row[sym].l;

            if (i === 0 || !df_4h[i - 1][sym]) {
                row.TR[sym] = null;
            } else {
                let prev_c = df_4h[i - 1][sym].c;
                row.TR[sym] = Math.max(h - l, Math.abs(h - prev_c), Math.abs(l - prev_c));
            }

            if (i >= params.VOL_WINDOW) {
                let tr_sum = 0;
                let valid_count = 0;
                for (let j = 0; j < params.VOL_WINDOW; j++) {
                    if (df_4h[i - j].TR[sym] !== null) {
                        tr_sum += df_4h[i - j].TR[sym];
                        valid_count++;
                    }
                }
                if (valid_count === params.VOL_WINDOW) {
                    let atr = tr_sum / params.VOL_WINDOW;
                    row.ATR[sym] = atr;
                    row.VOL[sym] = atr / c;
                }
            }

            if (i >= params.MOM_WINDOW) {
                if (df_4h[i - params.MOM_WINDOW][sym]) {
                    let past_c = df_4h[i - params.MOM_WINDOW][sym].c;
                    let mom = past_c > 0 ? (c - past_c) / past_c : 0;
                    row.MOM[sym] = mom;
                    if (row.VOL[sym] !== undefined && row.VOL[sym] > 0) {
                        row.ADJ_MOM[sym] = mom / (row.VOL[sym] + 1e-8);
                    } else {
                        row.ADJ_MOM[sym] = 0;
                    }
                }
            }
        }

        if (i >= min_warmup) {
            let current_time_obj = new Date(row.time + 4 * 3600 * 1000 + 8 * 3600 * 1000);
            let current_time = current_time_obj.toISOString().replace('T', ' ').substring(0, 19);
            let current_time_ms = row.time + 4 * 3600 * 1000;

            let top_long_coins = [];
            let top_short_coins = [];

            if (row.BTC_TREND_ON) {
                if (['BOTH', 'LONG_ONLY'].includes(trade_mode)) {
                    let eligible = [];
                    for (const sym of symbols) {
                        if (row.ADJ_MOM[sym] > 0) eligible.push({ sym: sym, val: row.ADJ_MOM[sym] });
                    }
                    eligible.sort((a, b) => b.val - a.val);
                    top_long_coins = eligible.slice(0, params.TOP_K).map(x => x.sym);
                }
            } else {
                if (['BOTH', 'SHORT_ONLY'].includes(trade_mode)) {
                    let eligible = [];
                    for (const sym of symbols) {
                        if (row.ADJ_MOM[sym] < 0) eligible.push({ sym: sym, val: row.ADJ_MOM[sym] });
                    }
                    eligible.sort((a, b) => a.val - b.val);
                    top_short_coins = eligible.slice(0, params.TOP_K).map(x => x.sym);
                }
            }

            if (params.START_TRADE_TIMESTAMP && current_time_ms < params.START_TRADE_TIMESTAMP) {
                top_long_coins = [];
                top_short_coins = [];
            }

            // --- A. 平仓逻辑 ---
            for (const sym of symbols) {
                let current_qty = positions[sym].qty;
                if (current_qty > 0 && !top_long_coins.includes(sym)) {
                    let reason = !row.BTC_TREND_ON ? "大盘开关关闭" : "掉出排名";
                    trade_logs.push({
                        time: current_time, action: "SELL", coin: sym.replace('USDT', ''), direction: "LONG", price: row[sym].c, reason: reason
                    });
                    positions[sym].qty = 0;
                }
                else if (current_qty < 0 && !top_short_coins.includes(sym)) {
                    let reason = row.BTC_TREND_ON ? "大盘开关关闭" : "掉出排名";
                    trade_logs.push({
                        time: current_time, action: "BUY", coin: sym.replace('USDT', ''), direction: "SHORT", price: row[sym].c, reason: reason
                    });
                    positions[sym].qty = 0;
                }
            }

            // --- B. 开仓逻辑 (多) ---
            if (top_long_coins.length > 0) {
                for (const sym of top_long_coins) {
                    if (positions[sym].qty === 0) {
                        trade_logs.push({
                            time: current_time, action: "BUY", coin: sym.replace('USDT', ''), direction: "LONG", price: row[sym].c, reason: "Signal Entry Long"
                        });
                        positions[sym].qty = 1;
                    }
                }
            }

            // --- C. 开仓逻辑 (空) ---
            if (top_short_coins.length > 0) {
                for (const sym of top_short_coins) {
                    if (positions[sym].qty === 0) {
                        trade_logs.push({
                            time: current_time, action: "SELL", coin: sym.replace('USDT', ''), direction: "SHORT", price: row[sym].c, reason: "Signal Entry Short"
                        });
                        positions[sym].qty = -1;
                    }
                }
            }
        }
    }
    return trade_logs;
}

const sleep = ms => new Promise(resolve => setTimeout(resolve, ms));

async function safeFetch(url) {
    const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
        'Accept': 'application/json'
    };
    try {
        console.log(`[Network] 🌐 发起 HTTP 请求: ${url.split('?')[0]}?${url.split('?')[1]?.substring(0, 50)}...`);
        const res = await fetch(url, { headers });
        const text = await res.text();
        try {
            const data = JSON.parse(text);
            if (!res.ok) {
                console.warn(`[Network] ⚠️ 接口返回非 200: HTTP ${res.status}`);
            }
            return { ok: res.ok, data: data, status: res.status };
        } catch (parseError) {
            console.error(`[Network] ❌ 解析 JSON 失败 (状态码 ${res.status}): `, text.substring(0, 100));
            return { ok: false, data: null, status: res.status };
        }
    } catch (networkError) {
        console.error(`[Network] ❌ 网络层彻底断开或超时:`, networkError.message);
        return { ok: false, data: null, status: 500 };
    }
}

// ==========================================
// 2. 全局防撞锁机制 (护城河)
// ==========================================
async function acquireGlobalLock(env) {
    console.log(`[Lock] 🔒 开始尝试获取全局防撞锁...`);
    await env.DB.prepare("CREATE TABLE IF NOT EXISTS sys_lock (id INTEGER PRIMARY KEY, is_locked INTEGER, lock_time INTEGER)").run();
    await env.DB.prepare("INSERT OR IGNORE INTO sys_lock (id, is_locked, lock_time) VALUES (1, 0, 0)").run();

    const now = Date.now();
    const timeout = now - 5 * 60 * 1000;

    const res = await env.DB.prepare(
        "UPDATE sys_lock SET is_locked = 1, lock_time = ? WHERE id = 1 AND (is_locked = 0 OR lock_time < ?)"
    ).bind(now, timeout).run();

    const isSuccess = res.meta.changes > 0;
    if (isSuccess) {
        console.log(`[Lock] ✅ 全局锁获取成功，允许执行任务`);
    } else {
        console.warn(`[Lock] 🚫 全局锁获取失败，当前系统有任务正在运行中`);
    }
    return isSuccess;
}

async function releaseGlobalLock(env) {
    console.log(`[Lock] 🔓 正在释放全局防撞锁...`);
    await env.DB.prepare("UPDATE sys_lock SET is_locked = 0 WHERE id = 1").run();
    console.log(`[Lock] ✅ 全局锁已安全释放`);
}


// ==========================================
// 3. 构建统一推演流水线 (Unified Pipeline) - 多策略重构版
// ==========================================
async function runUnifiedPipeline(env, ctx) {
    console.log(`[Pipeline] 🚀 ================= 开始执行统一推演流水线 =================`);

    const locked = await acquireGlobalLock(env);
    if (!locked) {
        console.warn(`[Pipeline] 🛑 阻断：撞锁，退出当前流水线执行`);
        return { isLockedOut: true, msg: "系统已有推演任务正在执行中，触发全局防撞锁阻断。" };
    }

    try {
        const targetSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT'];
        const MAX_CANDLES = 200000;
        let all_klines_map = {};
        let dbBatchUpdates = [];
        let hasRateLimitError = false;

        console.log(`[Data] 准备并发拉取币安数据，涉及币种数量: ${targetSymbols.length}`);

        for (const symbol of targetSymbols) {
            console.log(`[Data] ---------- 开始处理: ${symbol} ----------`);
            const { results } = await env.DB.prepare("SELECT data FROM kline_cache WHERE symbol LIKE ?").bind(`${symbol}%`).all();

            let cachedKlines = [];
            if (results && results.length > 0) {
                results.forEach(row => {
                    if (row.data) {
                        let parsed = JSON.parse(row.data);
                        for (let j=0; j<parsed.length; j++) cachedKlines.push(parsed[j]);
                    }
                });
                cachedKlines.sort((a, b) => a[0] - b[0]);
            }

            console.log(`[Data] [${symbol}] D1数据库缓存命中: ${cachedKlines.length} 根 K 线`);

            let klineMap = new Map();
            cachedKlines.forEach(k => klineMap.set(k[0], k));
            let fetchTasks = [];

            if (cachedKlines.length === 0) {
                console.log(`[Data] [${symbol}] 无缓存，构建冷启动全量抓取任务...`);
                fetchTasks.push(`https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=1500`);
            } else {
                let oldestTime = cachedKlines[0][0];
                let newestTime = cachedKlines[cachedKlines.length - 1][0];

                if (Date.now() - newestTime > 60000) {
                    fetchTasks.push(`https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=1500&startTime=${newestTime}`);
                }
                if (cachedKlines.length < MAX_CANDLES) {
                    fetchTasks.push(`https://fapi.binance.com/fapi/v1/klines?symbol=${symbol}&interval=1m&limit=1500&endTime=${oldestTime - 1}`);
                }
                console.log(`[Data] [${symbol}] 构建了 ${fetchTasks.length} 个增量/回溯抓取任务`);
            }

            for (const urlStr of fetchTasks) {
                const result = await safeFetch(urlStr);
                if (!result.ok || !Array.isArray(result.data)) {
                    hasRateLimitError = true;
                    console.warn(`[Data] [${symbol}] ⚠️ 触发币安接口限速或异常，跳过后续 API 请求`);
                    break;
                }
                result.data.forEach(k => {
                    klineMap.set(k[0], [k[0], parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5])]);
                });
                await sleep(400); // 防风控延迟
            }

            let mergedKlines = Array.from(klineMap.values()).sort((a, b) => a[0] - b[0]);
            if (mergedKlines.length > MAX_CANDLES) {
                mergedKlines = mergedKlines.slice(mergedKlines.length - MAX_CANDLES);
            }

            if (mergedKlines.length > cachedKlines.length) {
                console.log(`[Data] [${symbol}] 存在有效增量，将加入落盘队列。`);
                dbBatchUpdates.push({ symbol: symbol, data: mergedKlines });
            }
            all_klines_map[symbol] = mergedKlines;
            console.log(`[Data] [${symbol}] 合并后总数据量: ${mergedKlines.length} 根 K 线`);
        }

        if (dbBatchUpdates.length > 0) {
            console.log(`[DB] 💿 监测到 ${dbBatchUpdates.length} 个币种存在增量数据，提交至 ctx.waitUntil 后台异步落盘...`);
            ctx.waitUntil((async () => {
                for (let update of dbBatchUpdates) {
                    try {
                        let stmts = [];
                        stmts.push(env.DB.prepare("DELETE FROM kline_cache WHERE symbol LIKE ?").bind(`${update.symbol}%`));

                        const CHUNK_SIZE = 15000;
                        for (let i = 0; i < update.data.length; i += CHUNK_SIZE) {
                            let chunk = update.data.slice(i, i + CHUNK_SIZE);
                            let chunkSymbol = i === 0 ? update.symbol : `${update.symbol}_${i}`;
                            let lastUpdated = chunk[chunk.length - 1][0];

                            stmts.push(
                                env.DB.prepare("INSERT INTO kline_cache (symbol, data, last_updated) VALUES (?, ?, ?)")
                                .bind(chunkSymbol, JSON.stringify(chunk), lastUpdated)
                            );
                        }
                        await env.DB.batch(stmts);
                        console.log(`[DB] ✅ 币种 ${update.symbol} 分块写入成功，共切片 ${stmts.length - 1} 块`);
                    } catch (err) {
                        console.error(`[DB] ❌ 后台存入 ${update.symbol} 分块数据失败:`, err);
                    }
                }
            })());
        } else {
            console.log(`[DB] ℹ️ 没有增量数据需要落盘`);
        }

        console.log(`[Math] 🧠 开始核心量化引擎计算 (多策略并行循环推演)...`);

        const STRATEGIES = [
            {
                STRATEGY_ID: 'Grid_No.43629',
                TRADE_MODE: 'LONG_ONLY',
                MOM_WINDOW: 48,
                VOL_WINDOW: 42,
                BTC_TREND_WINDOW: 120,
                MAX_WEIGHT: 0.5,
                TOP_K: 1,
                TIME_OFFSET_MS: 2 * 3600 * 1000,
                START_TRADE_TIMESTAMP: new Date('2026-04-27T00:00:00+08:00').getTime()
            },
            {
                STRATEGY_ID: 'Grid_No.69393',
                TRADE_MODE: 'SHORT_ONLY',
                MOM_WINDOW: 90,
                VOL_WINDOW: 120,
                BTC_TREND_WINDOW: 720,
                MAX_WEIGHT: 0.05,
                TOP_K: 3,
                TIME_OFFSET_MS: 0,
                START_TRADE_TIMESTAMP: new Date('2026-04-27T00:00:00+08:00').getTime()
            }
        ];

        let fullSimulatedLogs = [];

        for (const strategy of STRATEGIES) {
            console.log(`[Math] ⚙️ 正在推演策略: ${strategy.STRATEGY_ID} (${strategy.TRADE_MODE})`);

            let df_4h_ready = alignAndResampleTo4H(all_klines_map, targetSymbols, strategy.TIME_OFFSET_MS);

            let logs = generateHistoricalTradeLogs(df_4h_ready, strategy, targetSymbols);

            logs.forEach(log => {
                log.strategy_id = strategy.STRATEGY_ID;
            });

            fullSimulatedLogs.push(...logs);
            console.log(`[Math] 策略 ${strategy.STRATEGY_ID} 推演完毕，产生 ${logs.length} 条记录。`);
        }

        console.log(`[Math] 矩阵推演结束，所有策略共产生 ${fullSimulatedLogs.length} 条原始交易日志。`);

        // 🔴 新增：计算真正的“共有时间交集”并写入最近推演时间
        console.log(`[DB] 💾 更新系统状态 (共有数据时间范围与最后计算时间)...`);
        let symbolMinTimes = [];
        let symbolMaxTimes = [];
        for (const sym of targetSymbols) {
            let klines = all_klines_map[sym];
            if (klines && klines.length > 0) {
                symbolMinTimes.push(klines[0][0]);
                symbolMaxTimes.push(klines[klines.length - 1][0]);
            }
        }
        // 共有时间开始 = 各币种开始时间的最大值；共有时间结束 = 各币种结束时间的最小值
        let commonDataStart = symbolMinTimes.length > 0 ? Math.max(...symbolMinTimes) : null;
        let commonDataEnd = symbolMaxTimes.length > 0 ? Math.min(...symbolMaxTimes) : null;
        let lastCalcTime = Date.now();

        await env.DB.prepare("CREATE TABLE IF NOT EXISTS sys_status (id INTEGER PRIMARY KEY, common_data_start INTEGER, common_data_end INTEGER, last_calc_time INTEGER)").run();
        await env.DB.prepare("INSERT OR REPLACE INTO sys_status (id, common_data_start, common_data_end, last_calc_time) VALUES (1, ?, ?, ?)").bind(commonDataStart, commonDataEnd, lastCalcTime).run();
        // -------------------------------------------------------------

        console.log(`[DB] 🔍 从 D1 拉取历史信号库进行指纹比对...`);
        const { results: existingLogs } = await env.DB.prepare("SELECT * FROM live_simulation_logs").all();
        const existingSet = new Set((existingLogs || []).map(l => `${l.strategy_id || 'UNKNOWN'}_${l.time}_${l.action}_${l.coin}`));
        console.log(`[DB] 历史信号库基数: ${existingSet.size} 条记录。开始去重过滤...`);

        const now_ms = Date.now();
        const newLogsToInsert = fullSimulatedLogs.filter(log => {
            const logTimeMs = new Date(log.time.replace(' ', 'T') + '+08:00').getTime();
            const isClosed = now_ms >= logTimeMs;
            const logKey = `${log.strategy_id}_${log.time}_${log.action}_${log.coin}`;
            return isClosed && !existingSet.has(logKey);
        });

        if (newLogsToInsert.length > 0) {
            console.log(`[DB] 💾 去重完成，准备写入 ${newLogsToInsert.length} 条全新信号记录！`);
            const stmts = newLogsToInsert.map(log => {
                return env.DB.prepare(
                    "INSERT INTO live_simulation_logs (strategy_id, time, action, coin, direction, price, reason) VALUES (?, ?, ?, ?, ?, ?, ?)"
                ).bind(log.strategy_id, log.time, log.action, log.coin, log.direction, log.price, log.reason);
            });
            await env.DB.batch(stmts);
            console.log(`[DB] ✅ ${newLogsToInsert.length} 条新信号已成功入库！`);
        } else {
            console.log(`[DB] ℹ️ 去重完成，当前无任何全新信号生成。`);
        }

        console.log(`[Pipeline] 🎉 ================= 流水线执行圆满结束 =================`);
        return { isLockedOut: false, partial_success: hasRateLimitError, msg: '推演完成。' };

    } catch (error) {
        console.error(`[Pipeline] ❌ 致命错误 (Pipeline Execution Error):`, error);
        throw error;
    } finally {
        await releaseGlobalLock(env);
    }
}


// ==========================================
// 4. 双轨触发路由 (HTTP + CRON / CQRS 严格执行)
// ==========================================

const CORS_HEADERS = {
    'Access-Control-Allow-Origin': '*',
    'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
    'Access-Control-Allow-Headers': 'Content-Type',
};

export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);

        if (request.method === 'OPTIONS') {
            return new Response(null, { headers: CORS_HEADERS });
        }

        console.log(`[Router] 🌐 收到 ${request.method} 请求: ${url.pathname}`);

        try {
            if (request.method === 'GET' && url.pathname === '/api/logs') {
                if (!env.DB) throw new Error("D1 数据库未绑定！");

                console.log(`[Router] 执行读链路 (CQRS Read)`);
                const { results } = await env.DB.prepare("SELECT * FROM live_simulation_logs ORDER BY time DESC LIMIT 100").all();

                // 🔴 修改：不再解析 JSON，直接从 sys_status 中获取最新计算的共享时间与推演时间
                let dataStart = null;
                let dataEnd = null;
                let lastCalcTime = null;
                try {
                    await env.DB.prepare("CREATE TABLE IF NOT EXISTS sys_status (id INTEGER PRIMARY KEY, common_data_start INTEGER, common_data_end INTEGER, last_calc_time INTEGER)").run();
                    const statusRes = await env.DB.prepare("SELECT * FROM sys_status WHERE id = 1").first();
                    if (statusRes) {
                        dataStart = statusRes.common_data_start;
                        dataEnd = statusRes.common_data_end;
                        lastCalcTime = statusRes.last_calc_time;
                    }
                } catch(e) { console.error(`[Router] ❌ 获取数据范围与系统状态失败:`, e); }

                console.log(`[Router] 响应 /api/logs: 成功返回 ${results?.length || 0} 条历史记录`);
                return new Response(JSON.stringify({
                    logs: results,
                    dataStart: dataStart,
                    dataEnd: dataEnd,
                    lastCalcTime: lastCalcTime // 返回新增的最近推演时间
                }), {
                    headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
                });
            }

            if (request.method === 'POST' && url.pathname === '/api/calculate') {
                if (!env.DB) throw new Error("D1 数据库未绑定！");

                console.log(`[Router] 执行写链路 (CQRS Write)，准备唤醒流水线`);
                const result = await runUnifiedPipeline(env, ctx);

                if (result.isLockedOut) {
                    console.warn(`[Router] 拦截返回: 返回 429 状态，命中防撞锁`);
                    return new Response(JSON.stringify({ error: result.msg }), {
                        status: 429,
                        headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
                    });
                }

                console.log(`[Router] 响应 /api/calculate: 成功`);
                return new Response(JSON.stringify({
                    status: 'success',
                    partial_success: result.partial_success,
                    msg: result.msg
                }), { headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' } });
            }

            console.warn(`[Router] ⚠️ 命中 404 Endpoint Not Found`);
            return new Response('Endpoint Not Found', { status: 404, headers: CORS_HEADERS });

        } catch (error) {
            console.error(`[Router] ❌ 全局路由异常:`, error);
            return new Response(JSON.stringify({ error: error.message, stack: error.stack }), {
                status: 500,
                headers: { ...CORS_HEADERS, 'Content-Type': 'application/json' }
            });
        }
    },

    async scheduled(event, env, ctx) {
        console.log(`[Cron] ⏰ =================================================`);
        console.log(`[Cron] ⏰ 定时器触发唤醒！ScheduledTime: ${event.scheduledTime}`);

        ctx.waitUntil(
            runUnifiedPipeline(env, ctx)
                .then(result => {
                    console.log(`[Cron] ✅ 定时后台推演任务圆满结束:`, result);
                })
                .catch(err => {
                    console.error(`[Cron] ❌ 定时执行后台推演任务抛出异常:`, err);
                })
        );
    }
};