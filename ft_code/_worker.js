// ==========================================
// 1. 前端 HTML 模板 (内嵌在 Worker 中)
// ==========================================
const HTML_CONTENT = `
<!DOCTYPE html>
<html lang="zh">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>币安永续合约 - 极速波动率看板</title>
    <script src="https://cdn.tailwindcss.com"></script>
</head>
<body class="bg-gray-900 text-white p-3 md:p-8 font-sans">
    <div class="max-w-5xl mx-auto">
        <div class="flex flex-col md:flex-row justify-between items-start md:items-center mb-6 gap-4 border-b border-gray-700 pb-4">
            <div class="w-full md:w-auto">
                <h1 class="text-2xl md:text-3xl font-bold text-yellow-500 flex items-center gap-2">
                    🚀 波动率排行榜 <span class="text-xs md:text-sm bg-gray-700 text-gray-300 px-2 py-1 rounded-full font-normal">Top 10</span>
                </h1>
                <p id="updateTime" class="text-green-400 mt-2 text-xs md:text-sm">🔄 数据状态: 等待拉取...</p>
                
                <div class="mt-4 flex items-center gap-2 bg-gray-800 p-2.5 rounded border border-gray-700 w-full max-w-md shadow-inner">
                    <label for="minutesInput" class="text-xs md:text-sm text-gray-300 font-semibold whitespace-nowrap">⏳ 时间维度(分钟):</label>
                    <input type="text" id="minutesInput" value="15, 30, 60" class="bg-gray-900 border border-gray-600 text-yellow-400 text-sm rounded px-3 py-1 w-full focus:outline-none focus:border-yellow-500 font-mono transition-colors">
                </div>
            </div>
            
            <div class="flex flex-col items-start md:items-end gap-2 w-full md:w-auto">
                <p id="currentTime" class="text-gray-400 text-xs md:text-sm font-mono"></p>
                <button id="refreshBtn" onclick="fetchData()" class="w-full md:w-auto justify-center bg-yellow-600 hover:bg-yellow-500 transition-colors px-6 py-2 rounded font-bold shadow-lg flex items-center gap-2 mt-2 md:mt-0">
                    ⚡ 实时拉取最新数据
                </button>
            </div>
        </div>

        <div id="errorBox" class="hidden p-4 rounded mb-6 text-sm"></div>

        <div class="overflow-x-auto rounded-lg border border-gray-700 shadow-xl scrollbar-thin scrollbar-thumb-gray-600 scrollbar-track-gray-800">
            <table class="w-full text-left border-collapse min-w-max">
                <thead class="bg-gray-800 text-gray-300 border-b border-gray-600">
                    <tr>
                        </tr>
                </thead>
                <tbody id="tableBody" class="bg-gray-900 divide-y divide-gray-800">
                    <tr><td colspan="10" class="p-8 text-center text-gray-500 text-sm md:text-base">点击右上角按钮开始实时拉取市场数据...</td></tr>
                </tbody>
            </table>
        </div>
    </div>

    <script>
        // 排序与全局数据状态
        let currentData = [];
        let defaultData = [];
        let currentMinutes = [15, 30, 60]; 
        let sortConfig = { key: 'overall', direction: 'desc' }; 

        function updateLocalTime() {
            const now = new Date();
            document.getElementById('currentTime').innerText = "当前系统时间: " + now.toLocaleTimeString();
        }
        setInterval(updateLocalTime, 1000);
        updateLocalTime();

        function formatDateTime(isoString) {
            if (!isoString) return "未知时间";
            const date = new Date(isoString);
            return date.toLocaleTimeString(); 
        }

        function handleSort(key) {
            if (sortConfig.key === key) {
                if (sortConfig.direction === 'desc') sortConfig.direction = 'asc';
                else if (sortConfig.direction === 'asc') sortConfig.direction = 'default';
                else sortConfig.direction = 'desc';
            } else {
                sortConfig.key = key;
                sortConfig.direction = 'desc';
            }
            renderTable();
        }

        function getSortIcon(key) {
            if (sortConfig.key !== key || sortConfig.direction === 'default') {
                return '<span class="text-gray-600 ml-1 text-xs opacity-50">↕</span>';
            }
            return sortConfig.direction === 'asc' 
                ? '<span class="text-yellow-500 ml-1 text-xs">▲</span>' 
                : '<span class="text-yellow-500 ml-1 text-xs">▼</span>';
        }

        function renderTable() {
            const tableBody = document.getElementById('tableBody');
            const theadTr = document.querySelector('thead tr');
            
            let theadHtml = '<th class="px-3 py-3 md:p-4 font-semibold w-10 md:w-12 text-center whitespace-nowrap">#</th>';
            theadHtml += '<th class="px-3 py-3 md:p-4 font-semibold whitespace-nowrap cursor-pointer select-none hover:text-white transition-colors" onclick="handleSort(\\'symbol\\')">交易对 ' + getSortIcon('symbol') + '</th>';
            theadHtml += '<th class="px-3 py-3 md:p-4 font-semibold whitespace-nowrap cursor-pointer select-none hover:text-white transition-colors" onclick="handleSort(\\'price\\')">最新价格 ' + getSortIcon('price') + '</th>';
            theadHtml += '<th class="px-3 py-3 md:p-4 font-semibold whitespace-nowrap text-yellow-400 cursor-pointer select-none hover:text-yellow-300 transition-colors" onclick="handleSort(\\'overall\\')">综合波动率 ' + getSortIcon('overall') + '</th>';
            
            currentMinutes.forEach(m => {
                theadHtml += '<th class="px-3 py-3 md:p-4 font-semibold whitespace-nowrap text-gray-400 cursor-pointer select-none hover:text-gray-300 transition-colors" onclick="handleSort(\\'' + m + 'm\\')">' + m + 'm 波动 ' + getSortIcon(m + 'm') + '</th>';
            });
            theadTr.innerHTML = theadHtml;

            let displayData = [...currentData];
            if (sortConfig.direction !== 'default') {
                displayData.sort((a, b) => {
                    let valA, valB;
                    if (sortConfig.key === 'symbol') {
                        return sortConfig.direction === 'asc' ? a.symbol.localeCompare(b.symbol) : b.symbol.localeCompare(a.symbol);
                    } else if (sortConfig.key === 'price') {
                        valA = parseFloat(a.price); valB = parseFloat(b.price);
                    } else if (sortConfig.key === 'overall') {
                        valA = parseFloat(a.overall); valB = parseFloat(b.overall);
                    } else {
                        valA = parseFloat(a.avgVols[sortConfig.key]); valB = parseFloat(b.avgVols[sortConfig.key]);
                    }
                    return sortConfig.direction === 'asc' ? valA - valB : valB - valA;
                });
            } else {
                displayData = [...defaultData]; 
            }

            if (displayData.length === 0) {
                tableBody.innerHTML = '<tr><td colspan="' + (4 + currentMinutes.length) + '" class="p-8 text-center text-gray-500 text-sm md:text-base">当前没有满足条件的波动率数据。</td></tr>';
                return;
            }

            tableBody.innerHTML = displayData.map((item, index) => {
                let rankStyle = "text-gray-400";
                if (sortConfig.direction === 'default' || (sortConfig.key === 'overall' && sortConfig.direction === 'desc')) {
                    if (index === 0) rankStyle = "text-yellow-400 font-bold text-base md:text-lg";
                    else if (index === 1) rankStyle = "text-gray-300 font-bold text-base md:text-lg";
                    else if (index === 2) rankStyle = "text-orange-400 font-bold text-base md:text-lg";
                }

                let html = '<tr class="hover:bg-gray-800 transition-colors">';
                html += '<td class="px-3 py-3 md:p-4 text-center whitespace-nowrap ' + rankStyle + '">' + (index + 1) + '</td>';
                html += '<td class="px-3 py-3 md:p-4 font-bold whitespace-nowrap text-sm md:text-base text-white">' + item.symbol + '</td>';
                html += '<td class="px-3 py-3 md:p-4 font-mono whitespace-nowrap text-sm md:text-base text-gray-300">' + item.price + '</td>';
                html += '<td class="px-3 py-3 md:p-4 font-mono whitespace-nowrap font-bold text-sm md:text-base text-green-400 bg-green-900/10">' + item.overall + '%</td>';
                
                currentMinutes.forEach(m => {
                    html += '<td class="px-3 py-3 md:p-4 font-mono whitespace-nowrap text-sm md:text-base text-gray-400">' + item.avgVols[m + 'm'] + '%</td>';
                });
                html += '</tr>';
                return html;
            }).join('');
        }

        function showErrorBox(htmlContent, isWarning = false) {
            const errorBox = document.getElementById('errorBox');
            errorBox.innerHTML = htmlContent;
            errorBox.className = isWarning 
                ? "bg-yellow-900/30 border border-yellow-600 text-yellow-200 p-4 rounded mb-6 text-sm" 
                : "bg-red-900/50 border border-red-500 text-red-200 p-4 rounded mb-6 text-sm";
            errorBox.classList.remove('hidden');
        }

        async function fetchData() {
            const btn = document.getElementById('refreshBtn');
            const errorBox = document.getElementById('errorBox');
            const updateTimeEl = document.getElementById('updateTime');
            const minutesInput = document.getElementById('minutesInput').value;
            
            btn.innerHTML = "⏳ <span class='hidden md:inline'>正在连接币安</span>拉取中...";
            btn.disabled = true;
            btn.classList.add('opacity-50', 'cursor-not-allowed');
            errorBox.classList.add('hidden');
            
            const tempColspan = 4 + currentMinutes.length;
            document.getElementById('tableBody').innerHTML = '<tr><td colspan="' + tempColspan + '" class="p-8 text-center text-yellow-500 text-sm md:text-base animate-pulse">正在并发拉取主力合约 K 线数据，请稍候...</td></tr>';

            try {
                const response = await fetch('/api/data?minutes=' + encodeURIComponent(minutesInput));
                const json = await response.json();

                if (!response.ok) {
                    throw new Error(json.error || \`HTTP 错误: \${response.status}\`);
                }

                if (!json.data || !Array.isArray(json.data)) {
                    throw new Error("未找到有效的数据列表");
                }

                // 更新全局数据并重置排序
                currentData = json.data;
                defaultData = [...json.data]; 
                // 如果后端返回了使用的 minutesList，则同步前端的列
                if (json.minutesList) {
                    currentMinutes = json.minutesList; 
                }
                sortConfig = { key: 'overall', direction: 'desc' }; 
                
                renderTable();

                // 【核心修改】：根据后端返回的 isCached 标识，展示不同状态
                if (json.isCached) {
                    showErrorBox(\`<strong>⚠️ 本次云端刷新失败:</strong> 后端拉取异常 (\${json.errorMsg || '未知原因'}) <br> <span class="block mt-2 font-semibold text-yellow-400">已为您加载 Cloudflare 云端历史数据 (数据最后更新于: \${formatDateTime(json.updateTime)})</span>\`, true);
                    updateTimeEl.innerHTML = \`⚠️ 正在展示云端历史缓存数据\`;
                    updateTimeEl.className = "text-yellow-500 mt-2 text-xs md:text-sm font-bold";
                } else {
                    updateTimeEl.innerHTML = \`✅ 数据拉取成功！最后更新: <span class="text-white font-mono">\${formatDateTime(json.updateTime)}</span>\`;
                    updateTimeEl.className = "text-green-400 mt-2 text-xs md:text-sm";
                }

            } catch (error) {
                console.error("请求失败详情:", error);
                showErrorBox(\`<strong>⚠️ 拉取失败:</strong> \${error.message} <br> <span class="text-xs text-red-300 mt-1 block">可能是由于触发了币安风控拦截，且云端无历史缓存可用。</span>\`);
                document.getElementById('tableBody').innerHTML = \`<tr><td colspan="\${tempColspan}" class="p-8 text-center text-red-500 text-sm md:text-base">获取数据失败，请稍后重试。</td></tr>\`;
                updateTimeEl.innerHTML = \`❌ 数据拉取失败\`;
                updateTimeEl.className = "text-red-400 mt-2 text-xs md:text-sm";
            } finally {
                btn.innerHTML = "⚡ <span class='hidden md:inline'>实时</span>拉取最新数据";
                btn.disabled = false;
                btn.classList.remove('opacity-50', 'cursor-not-allowed');
            }
        }

        renderTable();
        fetchData();
    </script>
</body>
</html>
`;


// ==========================================
// 2. Worker 路由分发器
// ==========================================
export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/") {
      return new Response(HTML_CONTENT, {
        headers: { "Content-Type": "text/html;charset=UTF-8" },
      });
    }

    if (url.pathname === "/api/data") {
      try {
        let minutesList = [15, 30, 60];
        const minutesParam = url.searchParams.get("minutes");
        if (minutesParam) {
            const parsed = minutesParam.split(',').map(s => parseInt(s.trim())).filter(n => !isNaN(n) && n > 0);
            if (parsed.length > 0) minutesList = parsed;
        }

        // 【核心修改】：传入 env 以便后端能够操作 KV 数据库
        const data = await updateVolatilityData(minutesList, env);
        data.minutesList = minutesList;

        return new Response(JSON.stringify(data), {
          headers: {
            "Content-Type": "application/json",
            "Cache-Control": "no-cache"
          },
        });
      } catch (error) {
        return new Response(JSON.stringify({ error: error.message }), {
          status: 500,
          headers: { "Content-Type": "application/json" }
        });
      }
    }

    return new Response("Not Found", { status: 404 });
  }
};


// ==========================================
// 3. 后端数据拉取核心逻辑 (加入抗并发的多代理池与云端 KV 降级)
// ==========================================

const PROXY_POOL = [
  "https://api.allorigins.win/raw?url=",
  "https://api.codetabs.com/v1/proxy?quest=",
  "https://corsproxy.io/?"
];

async function smartFetch(targetUrl) {
  const headers = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
    "Accept": "application/json"
  };

  let res = await fetch(targetUrl, { headers });

  if (res.status === 451 || res.status === 403) {
    const cacheBuster = targetUrl.includes('?') ? `&_t=${Date.now() + Math.random()}` : `?_t=${Date.now() + Math.random()}`;
    const finalUrl = targetUrl + cacheBuster;

    const shuffledProxies = [...PROXY_POOL].sort(() => 0.5 - Math.random());

    for (let proxyBase of shuffledProxies) {
       try {
           let proxyRes = await fetch(proxyBase + encodeURIComponent(finalUrl), { headers });
           if (proxyRes.ok) {
               return proxyRes;
           }
       } catch(e) {
           continue;
       }
    }
  }

  return res;
}

async function updateVolatilityData(minutesList, env) {
  const BASE_URL = "https://fapi.binance.com";

  try {
    const tickerRes = await smartFetch(`${BASE_URL}/fapi/v1/ticker/24hr`);
    if (!tickerRes.ok) throw new Error(`Ticker 接口请求失败: HTTP ${tickerRes.status}`);

    const tickers = await tickerRes.json();
    if (!Array.isArray(tickers)) throw new Error(`API 异常，可能代理节点限流，请稍后重试`);

    const currentTime = Date.now();

    let candidates = tickers
      .filter(t => {
          const isAlive = (currentTime - parseInt(t.closeTime || 0)) < 600000;
          const isUsdtPerp = t.symbol.endsWith('USDT') && !t.symbol.includes('_');
          const hasGoodVolume = t.quoteVolume && parseFloat(t.quoteVolume) > 20000000;
          return isAlive && isUsdtPerp && hasGoodVolume;
      })
      .map(t => ({
        symbol: t.symbol,
        roughVol: (parseFloat(t.highPrice) - parseFloat(t.lowPrice)) / parseFloat(t.lowPrice)
      }))
      .sort((a, b) => b.roughVol - a.roughVol)
      .slice(0, 10);

    const maxMinute = Math.max(...minutesList);
    const fetchLimit = Math.min(Math.max(maxMinute, 60), 1000);

    const fetchPromises = candidates.map(async (item) => {
      try {
        const klineRes = await smartFetch(`${BASE_URL}/fapi/v1/klines?symbol=${item.symbol}&interval=1m&limit=${fetchLimit}`);
        if (!klineRes.ok) return null;

        const klines = await klineRes.json();
        if (!Array.isArray(klines) || klines.length < maxMinute) return null;

        const minuteVols = klines.map(k => (parseFloat(k[2]) - parseFloat(k[3])) / parseFloat(k[3]));

        let symbolData = {
          symbol: item.symbol,
          price: klines[klines.length - 1][4],
          avgVols: {}
        };

        let totalAvg = 0;
        minutesList.forEach(m => {
          const slice = minuteVols.slice(-m);
          const avg = (slice.reduce((a, b) => a + Math.abs(b), 0) / m) * 100;
          symbolData.avgVols[`${m}m`] = avg.toFixed(4);
          totalAvg += avg;
        });

        symbolData.overall = (totalAvg / minutesList.length).toFixed(4);
        return symbolData;

      } catch (e) {
        return null;
      }
    });

    const rawResults = await Promise.all(fetchPromises);

    const results = rawResults.filter(r => r !== null && parseFloat(r.overall) > 0);
    results.sort((a, b) => parseFloat(b.overall) - parseFloat(a.overall));

    if (results.length === 0) {
       throw new Error("有效数据为 0，免费代理可能正处于高负荷状态。");
    }

    const finalData = {
      updateTime: new Date().toISOString(),
      data: results
    };

    // 【核心新增】：如果成功，将数据存入 Cloudflare KV 数据库
    if (env && env.VOLATILITY_KV) {
        // 使用 waitUntil 让存库操作在后台执行，不阻塞返回前端的速度
        env.VOLATILITY_KV.put("RANKING_DATA", JSON.stringify(finalData)).catch(e => console.error("KV 写入失败", e));
    }

    return finalData;

  } catch (err) {
    // 【核心新增】：如果拉取失败，尝试从 Cloudflare KV 读取历史数据作为降级展示
    if (env && env.VOLATILITY_KV) {
        try {
            const cachedDataStr = await env.VOLATILITY_KV.get("RANKING_DATA");
            if (cachedDataStr) {
                const cachedData = JSON.parse(cachedDataStr);
                cachedData.isCached = true; // 告诉前端这是旧数据
                cachedData.errorMsg = err.message; // 附带原始报错原因
                return cachedData;
            }
        } catch (kvErr) {
            console.error("KV 读取异常", kvErr);
        }
    }

    // 如果没有配置 KV 或者 KV 里完全没有历史数据，则直接抛出报错
    throw err; 
  }
}