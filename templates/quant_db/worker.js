// ==========================================
// 1. 前端视图层 (HTML/CSS/JS 常驻字符串)
// ==========================================
const HTML_CONTENT = `
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Serverless Quant System</title>
    <style>
        body { font-family: system-ui, -apple-system, sans-serif; background-color: #f3f4f6; color: #1f2937; padding: 20px; max-width: 1000px; margin: auto; }
        .header { display: flex; justify-content: space-between; align-items: center; margin-bottom: 20px; }
        button { background-color: #3b82f6; color: white; border: none; padding: 10px 20px; border-radius: 6px; cursor: pointer; font-weight: bold; transition: background-color 0.2s; }
        button:disabled { background-color: #9ca3af; cursor: not-allowed; }
        table { width: 100%; border-collapse: collapse; background: white; border-radius: 8px; overflow: hidden; box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1); }
        th, td { padding: 12px 15px; text-align: left; border-bottom: 1px solid #e5e7eb; }
        th { background-color: #f9fafb; font-weight: 600; }
        .action-BUY { color: #10b981; font-weight: bold; }
        .action-SELL { color: #ef4444; font-weight: bold; }
        #error-box { display: none; background-color: #fee2e2; color: #b91c1c; padding: 12px; border-radius: 6px; margin-bottom: 20px; border: 1px solid #f87171; white-space: pre-wrap; font-family: monospace; }
        .warning { color: #d97706; font-size: 0.9em; margin-top: 5px; }

        /* 新增：数据统计看板样式，微调至200px使得5个卡片更好自适应 */
        .stats-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 15px; margin-bottom: 20px; }
        .stat-card { background: white; padding: 15px; border-radius: 8px; box-shadow: 0 2px 4px rgba(0,0,0,0.05); border: 1px solid #e5e7eb; display: flex; flex-direction: column; }
        .stat-title { font-size: 0.85em; color: #6b7280; margin-bottom: 8px; font-weight: 600; }
        .stat-value { font-size: 1.4em; font-weight: bold; color: #111827; }
        .stat-value-small { font-size: 1em; color: #374151; font-weight: 600; margin-top: auto; }
    </style>
</head>
<body>
    <div id="error-box"></div>
    <div class="header">
        <div>
            <h2>信号推演流水 (Live Logs)</h2>
            <div class="warning">⚠️ 注意：每次点击请间隔 3-5 秒，避免触发币安接口风控</div>
        </div>
        <button id="calcBtn" onclick="triggerCalculation()">逐步拉取增量数据并回测</button>
    </div>

    <!-- 新增：信号统计看板容器 -->
    <div class="stats-grid" id="statsGrid"></div>

    <table>
        <thead>
            <tr>
                <th>时间 (Time)</th>
                <th>动作 (Action)</th>
                <th>资产 (Coin)</th>
                <th>方向 (Direction)</th>
                <th>价格 (Price)</th>
                <th>触发原因 (Reason)</th>
            </tr>
        </thead>
        <tbody id="logTableBody">
            </tbody>
    </table>

    <script>
        window.onload = () => fetchLogs();

        async function fetchLogs() {
            try {
                const res = await fetch('/api/logs');
                if (!res.ok) {
                    const errorText = await res.text();
                    try {
                        const errJson = JSON.parse(errorText);
                        throw new Error(errJson.error || '未知后端错误');
                    } catch(e) {
                        throw new Error('服务器崩溃，返回了非 JSON 内容: ' + errorText.substring(0, 100));
                    }
                }
                const responseData = await res.json();

                let logs = [];
                let dataStart = null;
                let dataEnd = null;

                // 兼容数组或包装对象的数据结构
                if (Array.isArray(responseData)) {
                    logs = responseData;
                } else {
                    logs = responseData.logs || [];
                    dataStart = responseData.dataStart;
                    dataEnd = responseData.dataEnd;
                }

                const tbody = document.getElementById('logTableBody');

                if (logs.length === 0) {
                    tbody.innerHTML = '<tr><td colspan="6" style="text-align:center; color:#6b7280;">暂无交易流水，请继续点击按钮预热数据（需满 20 天）</td></tr>';
                    // 即使流水为空，也继续渲染看板（用于展示当前爬取到的数据范围）
                    renderStats(logs, dataStart, dataEnd);
                    return;
                }

                // 渲染看板统计数据
                renderStats(logs, dataStart, dataEnd);

                tbody.innerHTML = logs.map(log =>
                    '<tr>' +
                        '<td>' + log.time + '</td>' +
                        '<td class="action-' + log.action + '">' + log.action + '</td>' +
                        '<td>' + log.coin + '</td>' +
                        '<td>' + log.direction + '</td>' +
                        '<td>' + log.price.toFixed(4) + '</td>' +
                        '<td>' + log.reason + '</td>' +
                    '</tr>'
                ).join('');
            } catch (err) {
                showError('获取历史流水失败：\\n' + err.message);
            }
        }

        // 新增：渲染统计看板的核心逻辑
        function renderStats(logs, dataStart, dataEnd) {
            let buys = 0;
            let sells = 0;
            let currentHoldings = new Set();
            let processedCoins = new Set();

            // 日志已经是按时间倒序排的 (最新操作在最前面)
            for (let log of logs) {
                if (log.action === 'BUY') buys++;
                if (log.action === 'SELL') sells++;

                // 推断当前持仓：遍历时第一次遇到该币种的操作，如果是BUY说明目前正拿着，如果是SELL说明已平仓
                if (!processedCoins.has(log.coin)) {
                    processedCoins.add(log.coin);
                    if (log.action === 'BUY') {
                        currentHoldings.add(log.coin);
                    }
                }
            }

            const holdingsText = currentHoldings.size > 0
                ? Array.from(currentHoldings).map(c => '<span class="action-BUY">' + c + '</span>').join(', ')
                : '无 (全量空仓)';

            const latestTradeTime = logs.length > 0 ? logs[0].time : '无记录';

            // 时间格式化(与后端逻辑对齐: UTC+8)
            const formatTime = (ts) => {
                if (!ts || isNaN(ts)) return '未知';
                try {
                    let d = new Date(ts + 8 * 3600 * 1000);
                    return d.toISOString().replace('T', ' ').substring(0, 19);
                } catch(e) {
                    return '时间解析错误';
                }
            };

            const dataRangeText = (dataStart && dataEnd)
                ? formatTime(dataStart) + '<br><span style="font-size:0.8em;color:#6b7280;">至</span><br>' + formatTime(dataEnd)
                : '暂无回测数据';

            const statsHtml = \`
                <div class="stat-card">
                    <div class="stat-title">📊 原始数据时间范围</div>
                    <div class="stat-value-small" style="line-height:1.4;">\${dataRangeText}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">📈 记录内信号总数</div>
                    <div class="stat-value">\${logs.length} <span style="font-size: 0.5em; color: #9ca3af; font-weight: normal;">(上限100条)</span></div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">⚖️ 动作统计 (买 / 卖)</div>
                    <div class="stat-value">
                        <span class="action-BUY">\${buys}</span> / <span class="action-SELL">\${sells}</span>
                    </div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">💼 当前系统持仓推测</div>
                    <div class="stat-value-small">\${holdingsText}</div>
                </div>
                <div class="stat-card">
                    <div class="stat-title">⏱️ 最后一次触发时间</div>
                    <div class="stat-value-small">\${latestTradeTime}</div>
                </div>
            \`;
            document.getElementById('statsGrid').innerHTML = statsHtml;
        }

        async function triggerCalculation() {
            const btn = document.getElementById('calcBtn');
            const errorBox = document.getElementById('error-box');

            btn.disabled = true;
            btn.innerText = '⏳ 正在限速拉取并推演中...';
            btn.style.backgroundColor = '#f59e0b';
            errorBox.style.display = 'none';

            try {
                const res = await fetch('/api/calculate', { method: 'POST' });

                if (!res.ok) {
                    const errorText = await res.text();
                    try {
                        const errJson = JSON.parse(errorText);
                        throw new Error(errJson.error || '计算接口执行异常');
                    } catch(e) {
                        throw new Error('触发计算失败，状态码: ' + res.status + ' \\n服务器可能被币安风控拦截，请等待 1 分钟后再试。');
                    }
                }

                const result = await res.json();
                if (result.partial_success) {
                    showError('⚠️ 部分请求被币安拦截 (状态码 503/429)。已保存成功拉取的部分数据。建议稍作休息再点。');
                }

                await fetchLogs();
            } catch (err) {
                showError('推演执行报错：\\n' + err.message);
            } finally {
                // 强制前端冷却 3 秒，防止连续狂点
                setTimeout(() => {
                    btn.disabled = false;
                    btn.innerText = '逐步拉取增量数据并回测';
                    btn.style.backgroundColor = '#3b82f6';
                }, 3000);
            }
        }

        function showError(msg) {
            const box = document.getElementById('error-box');
            box.innerText = msg;
            box.style.display = 'block';
        }
    </script>
</body>
</html>
`;

// ==========================================
// 2. 核心数学与矩阵推演引擎
// ==========================================

// ==========================================
// 替换二：量化引擎的核心对齐函数 (覆盖原有的 alignAndResampleTo4H)
// ==========================================
function alignAndResampleTo4H(all_klines_map, symbols) {
    let chunkMap = new Map();
    const FOUR_HOURS_MS = 4 * 3600 * 1000;
    let minTime = Infinity;
    let maxTime = 0;

    // 1. 遍历收集数据，并记录全局时间轴跨度
    for (const sym of symbols) {
        if (!all_klines_map[sym]) continue;
        for (const k of all_klines_map[sym]) {
            let ts_4h = Math.floor(k[0] / FOUR_HOURS_MS) * FOUR_HOURS_MS;
            if (ts_4h < minTime) minTime = ts_4h;
            if (ts_4h > maxTime) maxTime = ts_4h;

            if (!chunkMap.has(ts_4h)) chunkMap.set(ts_4h, { time: ts_4h });
            let chunk = chunkMap.get(ts_4h);

            // ⚠️ 核心重构：完美复刻 Python Pandas 的 "Bug/特性"
            // 因为你本地是 df['close'].resample()，所以 4H 的最高/最低价其实是 1m 收盘价的极值
            if (!chunk[sym]) {
                chunk[sym] = { o: k[4], h: k[4], l: k[4], c: k[4] };
            } else {
                chunk[sym].h = Math.max(chunk[sym].h, k[4]);
                chunk[sym].l = Math.min(chunk[sym].l, k[4]);
                chunk[sym].c = k[4];
            }
        }
    }

    // 2. 补齐空缺时间 (复刻 Pandas resample 自动生成连续时间轴的特性)
    let sortedChunks = [];
    if (minTime !== Infinity) {
        for (let t = minTime; t <= maxTime; t += FOUR_HOURS_MS) {
            if (chunkMap.has(t)) {
                sortedChunks.push(chunkMap.get(t));
            } else {
                sortedChunks.push({ time: t }); // 强行塞入空缺的时间块
            }
        }
    }

    // 3. 前向填充 (复刻 ffill())
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

            // 🟢 修复二：对齐 Pandas shift(1) 产生的 NaN，第 0 行为 null
            if (i === 0 || !df_4h[i - 1][sym]) {
                row.TR[sym] = null;
            } else {
                let prev_c = df_4h[i - 1][sym].c;
                row.TR[sym] = Math.max(h - l, Math.abs(h - prev_c), Math.abs(l - prev_c));
            }

            // 🟢 修复二：对应计算 ATR 的循环容错处理，需集齐完整参数期才算数
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
            // 🟢 修复四：修正量化执行的时间锚点 (+4小时对齐闭合时刻)
            // 原因：row.time 是 4H 周期的起点(开盘)，但由于必须等 K 线收盘才能计算信号，真实的执行时间应当是收盘时刻。
            // 这一步不仅能纠正你看到的“时间晚/慢了4小时”的视觉偏差，
            // 还能完美联动修复底部 `isClosed` 无法拦截未走完“早产信号”的严重业务 Bug。
            let current_time_obj = new Date(row.time + 4 * 3600 * 1000 + 8 * 3600 * 1000);
            let current_time = current_time_obj.toISOString().replace('T', ' ').substring(0, 19);

            let top_long_coins = [];

            if (row.BTC_TREND_ON) {
                let eligible = [];
                for (const sym of symbols) {
                    if (row.ADJ_MOM[sym] > 0) eligible.push({ sym: sym, val: row.ADJ_MOM[sym] });
                }
                eligible.sort((a, b) => b.val - a.val);
                top_long_coins = eligible.slice(0, params.TOP_K).map(x => x.sym);
            }

            for (const sym of symbols) {
                if (positions[sym].qty > 0 && !top_long_coins.includes(sym)) {
                    let reason = !row.BTC_TREND_ON ? "大盘开关关闭" : "掉出排名";
                    trade_logs.push({
                        time: current_time, action: "SELL", coin: sym.replace('USDT', ''), direction: "LONG", price: row[sym].c, reason: reason
                    });
                    positions[sym].qty = 0;
                }
            }

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
        const res = await fetch(url, { headers });
        const text = await res.text();

        try {
            const data = JSON.parse(text);
            return { ok: res.ok, data: data, status: res.status };
        } catch (parseError) {
            console.error(`返回的非 JSON 内容 (状态码 ${res.status}): `, text.substring(0, 100));
            return { ok: false, data: null, status: res.status };
        }
    } catch (networkError) {
        return { ok: false, data: null, status: 500 };
    }
}

// ==========================================
// 3. 路由分发与主工作流
// ==========================================
export default {
    async fetch(request, env, ctx) {
        const url = new URL(request.url);

        try {
            if (request.method === 'GET' && url.pathname === '/') {
                return new Response(HTML_CONTENT, { headers: { 'Content-Type': 'text/html;charset=UTF-8' } });
            }

            if (request.method === 'GET' && url.pathname === '/api/logs') {
                if (!env.DB) throw new Error("D1 数据库未绑定！");

                // 拉取操作日志
                const { results } = await env.DB.prepare(
                    "SELECT * FROM live_simulation_logs ORDER BY time DESC LIMIT 100"
                ).all();

                // 新增：提取整个 kline_cache 缓存大盘数据的宏观起始/截止时间
                let dataStart = null;
                let dataEnd = null;
                try {
                    // 查询所有已存库数据的最晚时间戳 (截止时间)
                    const maxRes = await env.DB.prepare("SELECT MAX(last_updated) as maxT FROM kline_cache").first();
                    if (maxRes && maxRes.maxT) dataEnd = maxRes.maxT;

                    // 巧妙获取第一段分片的起始时间戳，无需展开所有大表 (以 BTCUSDT 零号分片为起算点)
                    const minChunk = await env.DB.prepare("SELECT data FROM kline_cache WHERE symbol = 'BTCUSDT'").first();
                    if (minChunk && minChunk.data) {
                        const parsed = JSON.parse(minChunk.data);
                        if (parsed.length > 0) dataStart = parsed[0][0]; // 提取第一根 K 线的开盘时间
                    }
                } catch(e) {
                    console.error("获取数据范围失败", e);
                }

                // 组装并返回
                return new Response(JSON.stringify({
                    logs: results,
                    dataStart: dataStart,
                    dataEnd: dataEnd
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            if (request.method === 'POST' && url.pathname === '/api/calculate') {
                if (!env.DB) throw new Error("D1 数据库未绑定！");

                const targetSymbols = ['BTCUSDT', 'ETHUSDT', 'SOLUSDT', 'XRPUSDT', 'BNBUSDT', 'DOGEUSDT'];
                // 🟢 修复三：调大回溯窗口，解决仓位状态滑动丢失问题
                const MAX_CANDLES = 200000;
                let all_klines_map = {};
                let dbBatchUpdates = [];
                let hasRateLimitError = false;

                for (const symbol of targetSymbols) {
                    const { results } = await env.DB.prepare(
                        "SELECT data FROM kline_cache WHERE symbol LIKE ?"
                    ).bind(`${symbol}%`).all();

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

                    let klineMap = new Map();
                    cachedKlines.forEach(k => klineMap.set(k[0], k));
                    let fetchTasks = [];

                    if (cachedKlines.length === 0) {
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
                    }

                    for (const urlStr of fetchTasks) {
                        const result = await safeFetch(urlStr);
                        if (!result.ok || !Array.isArray(result.data)) {
                            hasRateLimitError = true;
                            break;
                        }
                        result.data.forEach(k => {
                            klineMap.set(k[0], [k[0], parseFloat(k[1]), parseFloat(k[2]), parseFloat(k[3]), parseFloat(k[4]), parseFloat(k[5])]);
                        });
                        await sleep(400);
                    }

                    let mergedKlines = Array.from(klineMap.values()).sort((a, b) => a[0] - b[0]);
                    if (mergedKlines.length > MAX_CANDLES) {
                        mergedKlines = mergedKlines.slice(mergedKlines.length - MAX_CANDLES);
                    }

                    if (mergedKlines.length > cachedKlines.length) {
                        dbBatchUpdates.push({ symbol: symbol, data: mergedKlines });
                    }
                    all_klines_map[symbol] = mergedKlines;
                }

                if (dbBatchUpdates.length > 0) {
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
                            } catch (err) {
                                console.error(`后台存入 ${update.symbol} 分块数据失败:`, err);
                            }
                        }
                    })());
                }

                const BEST_PARAMS = {
                    MOM_WINDOW: 36,
                    VOL_WINDOW: 90,
                    BTC_TREND_WINDOW: 120,
                    MAX_WEIGHT: 0.4,
                    TOP_K: 1
                };

                let df_4h_ready = alignAndResampleTo4H(all_klines_map, targetSymbols);
                let fullSimulatedLogs = generateHistoricalTradeLogs(df_4h_ready, BEST_PARAMS, targetSymbols);

                // 🟢 核心修改：废弃 maxTime 拦截器，改为拉取数据库现有日志指纹进行精准去重
                const { results: existingLogs } = await env.DB.prepare("SELECT time, action, coin FROM live_simulation_logs").all();
                const existingSet = new Set((existingLogs || []).map(l => `${l.time}_${l.action}_${l.coin}`));

                // 🟢 确保不把由于提前点击产生的未跑完截面孤岛信号（早产信号）入库
                const now_ms = Date.now();
                const newLogsToInsert = fullSimulatedLogs.filter(log => {
                    const logTimeMs = new Date(log.time.replace(' ', 'T') + '+08:00').getTime();
                    const isClosed = now_ms >= logTimeMs;

                    // 生成唯一指纹，只要这笔特定操作没存入过数据库，就允许存入
                    const logKey = `${log.time}_${log.action}_${log.coin}`;
                    return isClosed && !existingSet.has(logKey);
                });

                if (newLogsToInsert.length > 0) {
                    const stmts = newLogsToInsert.map(log => {
                        return env.DB.prepare(
                            "INSERT INTO live_simulation_logs (time, action, coin, direction, price, reason) VALUES (?, ?, ?, ?, ?, ?)"
                        ).bind(log.time, log.action, log.coin, log.direction, log.price, log.reason);
                    });
                    await env.DB.batch(stmts);
                }

                return new Response(JSON.stringify({
                    status: 'success',
                    partial_success: hasRateLimitError,
                    msg: '推演完成。'
                }), { headers: { 'Content-Type': 'application/json' } });
            }

            return new Response('Endpoint Not Found', { status: 404 });

        } catch (error) {
            return new Response(JSON.stringify({
                error: error.message,
                stack: error.stack
            }), {
                status: 500,
                headers: { 'Content-Type': 'application/json' }
            });
        }
    }
};