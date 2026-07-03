import React, {useState, useEffect, useMemo, useRef} from 'react';
import {motion, AnimatePresence} from 'framer-motion';
import {
    Radar, Search, Link2, Layers, TrendingDown, Clock, Zap,
    ShieldAlert, ShieldCheck, AlertTriangle, CheckCircle2,
    ChevronDown, ArrowLeft, XCircle, Flame,
} from 'lucide-react';

/* =========================================================================
 * 0. 设计常量（苹果风）
 * ========================================================================= */
const FONT_STACK = '-apple-system, BlinkMacSystemFont, "SF Pro Display", "SF Pro Text", "Helvetica Neue", "PingFang SC", "Hiragino Sans GB", "Microsoft YaHei", sans-serif';
const EASE = [0.16, 1, 0.3, 1];
const GRAIN = "url(\"data:image/svg+xml,%3Csvg xmlns='http://www.w3.org/2000/svg' width='120' height='120'%3E%3Cfilter id='n'%3E%3CfeTurbulence type='fractalNoise' baseFrequency='0.85' numOctaves='4' stitchTiles='stitch'/%3E%3CfeColorMatrix type='saturate' values='0'/%3E%3C/filter%3E%3Crect width='100%25' height='100%25' filter='url(%23n)'/%3E%3C/svg%3E\")";

/* =========================================================================
 * 1. 配置 & 样式字典
 * ========================================================================= */
const LEVEL_STYLES = {
    high: {
        label: '高危',
        hex: '#ff453a',
        Icon: ShieldAlert,
        text: 'text-red-400',
        border: 'border-red-500/20',
        strip: 'bg-red-500/10 border-red-500/20',
        badge: 'bg-red-500/15 text-red-300'
    },
    medium: {
        label: '警惕',
        hex: '#ff9f0a',
        Icon: AlertTriangle,
        text: 'text-amber-400',
        border: 'border-amber-500/20',
        strip: 'bg-amber-500/10 border-amber-500/20',
        badge: 'bg-amber-500/15 text-amber-300'
    },
    low: {
        label: '稳健',
        hex: '#30d158',
        Icon: ShieldCheck,
        text: 'text-emerald-400',
        border: 'border-emerald-500/15',
        strip: 'bg-emerald-500/10 border-emerald-500/20',
        badge: 'bg-emerald-500/15 text-emerald-300'
    },
};

const OVERVIEW = {
    high: {word: '高危交易特征', advice: '谨慎跟单'},
    medium: {word: '需保持警惕', advice: '跟单前请看清下方数据'},
    low: {word: '暂无明显危险', advice: '仍需你独立判断'},
};

const LOADING_STEPS = [
    '正在调取历史真实成交…',
    '还原每一次加仓与平仓…',
    '还原历史真实的巨亏时刻…',
    '核算真实盈亏与持仓习惯…',
    '生成你的跟单风险体检报告…',
];

/* =========================================================================
 * 2. 工具函数
 * ========================================================================= */
const fmt = {
    num: (n, d = 2) => n == null ? '—' : Number(n).toLocaleString('en-US', {maximumFractionDigits: d}),
    int: (n) => n == null ? '—' : Number(n).toLocaleString('en-US', {maximumFractionDigits: 0}),
    pct: (n, d = 1) => n == null ? '—' : `${Number(n).toFixed(d)}%`,
    usd: (n) => n == null ? '—' : Number(n).toLocaleString('en-US', {maximumFractionDigits: 2}),
    price: (n) => {
        if (n == null) return '—';
        const a = Math.abs(n);
        const s = a >= 100 ? n.toFixed(2) : a >= 1 ? n.toFixed(4) : n.toFixed(6);
        return s.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
    },
    qty: (n) => n == null ? '—' : n >= 1 ? n.toLocaleString('en-US', {maximumFractionDigits: 3}) : String(n),
    date: (ts) => ts ? new Intl.DateTimeFormat('zh-CN', {
        year: 'numeric',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        hour12: false
    }).format(new Date(ts)).replace(/\//g, '-') : '—',
    duration: (ms) => {
        if (ms == null) return '—';
        const t = [[86400000, '天'], [3600000, '小时'], [60000, '分钟'], [1000, '秒']];
        for (let [u, label] of t) if (ms >= u) return `${(ms / u).toFixed(1)} ${label}`;
        return `${Math.round(ms)} 毫秒`;
    },
};

const evTime = (t, sec = false) => {
    if (t?.orderUpdateTime != null) {
        const d = new Date(t.orderUpdateTime);
        if (isNaN(d)) return '—';
        return new Intl.DateTimeFormat('zh-CN', {
            month: '2-digit',
            day: '2-digit',
            hour: '2-digit',
            minute: '2-digit', ...(sec ? {second: '2-digit'} : {}),
            hour12: false
        }).format(d).replace(/\//g, '-');
    }
    if (t?.orderUpdateTime_str) return sec ? t.orderUpdateTime_str.slice(5, 19) : t.orderUpdateTime_str.slice(5, 16);
    return '—';
};

const calcScore = (v, [lowMax, highMin, maxVal]) => {
    if (v == null) return 0;
    let s = v <= lowMax ? (v / lowMax) * 33 : v <= highMin ? 33 + ((v - lowMax) / (highMin - lowMax)) * 33 : 66 + ((v - highMin) / (maxVal - highMin)) * 34;
    return Math.max(0, Math.min(100, s));
};
const getLevel = (s) => (s >= 67 ? 'high' : s >= 34 ? 'medium' : 'low');

function useCountUp(target, duration = 1400) {
    const [val, setVal] = useState(0);
    useEffect(() => {
        let raf, start = performance.now();
        const tick = (now) => {
            const p = Math.min(1, (now - start) / duration);
            setVal(target * (1 - Math.pow(1 - p, 3)));
            p < 1 ? raf = requestAnimationFrame(tick) : setVal(target);
        };
        raf = requestAnimationFrame(tick);
        return () => cancelAnimationFrame(raf);
    }, [target, duration]);
    return val;
}

/* =========================================================================
 * 3. 维度配置字典
 * ========================================================================= */
const RISK_CONFIG = [
    {
        key: 'martingale', title: '越亏越加仓', term: '马丁格尔', icon: Layers,
        plain: '亏损时不断加倍仓位，扛过小赚，扛不过爆仓归零',
        extractValue: (d) => d?.martingale?.summary?.martingale_rate_percent,
        thresholds: [10, 25, 60],
        buildNode: (d) => {
            const s = d.martingale.summary;
            const seqs = d.martingale.evidences || [];
            let maxLen = 0, maxMult = 0;
            seqs.forEach(seq => {
                if (seq.length > maxLen) maxLen = seq.length;
                const first = seq[0]?.executedQty || 0;
                const total = seq.reduce((a, t) => a + (t.executedQty || 0), 0);
                if (first > 0) maxMult = Math.max(maxMult, total / first);
            });
            return {
                headline: (hl) => maxLen > 1
                    ? <>危险加仓占比 <b className={hl}>{fmt.pct(s.martingale_rate_percent)}</b>，最凶一次连续加仓 <b
                        className={hl}>{maxLen} 笔</b>，仓位放大至初始的 <b
                        className={`${hl} text-lg font-bold`}>{maxMult.toFixed(1)} 倍</b></>
                    : <>危险加仓占比 <b className={hl}>{fmt.pct(s.martingale_rate_percent)}</b>，共 <b
                        className={hl}>{fmt.int(s.martingale_sequences)} 次</b>「越亏越加」</>,
                stats: [],
                Component: MartingaleEvidence, data: d.martingale,
            };
        }
    },
    {
        key: 'tail_risk', title: '赢小亏大', term: '尾部风险', icon: TrendingDown,
        plain: '靠小额盈利堆高胜率，一次巨亏就吃掉数月利润',
        extractValue: (d) => d?.tail_risk?.summary?.tail_risk_index,
        thresholds: [20, 50, 150],
        buildNode: (d) => {
            const s = d.tail_risk.summary;
            return {
                headline: (hl) => <>单笔最大亏损 <b className={hl}>-{fmt.usd(s.max_single_loss)} U</b>，单笔中位盈利 <b
                    className={hl}>{fmt.num(s.median_single_profit, 2)} U</b>，要 <b
                    className={hl}>{fmt.int(s.tail_risk_index)} 笔</b>盈利才填得平</>,
                stats: [],
                Component: TailRiskEvidence, data: d.tail_risk,
            };
        }
    },
    {
        key: 'slippage_trap', title: '闪电平仓', term: '滑点风险', icon: Zap,
        plain: '几秒极速平仓，你的跟单成交时价格早已变天',
        extractValue: (d) => d?.slippage_trap?.summary?.slippage_trap_ratio_percent,
        thresholds: [8, 18, 40],
        buildNode: (d) => {
            const s = d.slippage_trap.summary;
            const evs = d.slippage_trap.evidences || [];
            const fastest = evs.reduce((m, e) => (e.hold_time_ms != null && e.hold_time_ms < m ? e.hold_time_ms : m), Infinity);
            return {
                headline: (hl) => <>最快 <b className={hl}>{fastest === Infinity ? '—' : fmt.duration(fastest)}</b>就平仓，<b
                    className={hl}>{fmt.pct(s.slippage_trap_ratio_percent)}</b> 的交易都这样快进快出，平均持仓 <b
                    className={hl}>{s.average_hold_time_minutes != null ? `${fmt.num(s.average_hold_time_minutes, 0)} 分钟` : '—'}</b></>,
                stats: [],
                Component: SlippageEvidence, data: d.slippage_trap,
            };
        }
    },
    {
        key: 'vw_hold_ratio', title: '赚就跑 · 亏死扛', term: '持仓纪律', icon: Clock,
        plain: '盈利急着落袋，亏损却长期死扛不认赔',
        extractValue: (d) => d?.vw_hold_ratio?.vw_hold_ratio,
        thresholds: [1.1, 1.5, 3],
        buildNode: (d) => {
            const v = d.vw_hold_ratio;
            return {
                headline: (hl) => <>亏钱时死扛的时间，是赚钱时的 <b
                    className={hl}>约 {fmt.num(v.vw_hold_ratio, 1)} 倍</b></>,
                stats: [
                    {label: '亏损 / 盈利持仓比', value: `×${fmt.num(v.vw_hold_ratio, 2)}`},
                    {label: '盈利平均持仓', value: fmt.duration(v.avg_profit_hold_time_ms)},
                    {label: '亏损平均持仓', value: fmt.duration(v.avg_loss_hold_time_ms)},
                ],
                Component: HoldRatioEvidence, data: v,
            };
        }
    }
];

const buildDimensions = (data) => !data ? [] : RISK_CONFIG.reduce((acc, cfg) => {
    const val = cfg.extractValue(data);
    if (val != null) {
        const score = calcScore(val, cfg.thresholds);
        acc.push({...cfg, score, level: getLevel(score), ...cfg.buildNode(data)});
    }
    return acc;
}, []);

/* =========================================================================
 * 5. 状态管理 Hook
 * ========================================================================= */
function useRiskAnalysis() {
    const [state, setState] = useState({status: 'idle', report: null, error: '', target: ''});
    const lastUrl = useRef('');
    const run = async (url) => {
        lastUrl.current = url;
        setState({status: 'loading', report: null, error: '', target: url});

        // 方案一：动态识别环境，自动切换 API 路由
        let backendUrl = "";
        if (window.location.hostname === "127.0.0.1" || window.location.hostname === "localhost") {
            // 1. 本地开发环境
            backendUrl = "http://localhost:8000/api/report";
        } else {
            // 2. 云服务器生产环境 (使用相对路径，由 Nginx/Web服务器 自动转发)
            backendUrl = "/api/report";
        }

        try {
            const res = await fetch(backendUrl, {
                method: 'POST', headers: {'Content-Type': 'application/json'}, body: JSON.stringify({url}),
            });
            if (!res.ok) throw new Error(`服务器返回错误状态码：${res.status}`);
            setState({status: 'success', report: await res.json(), error: '', target: url});
        } catch (err) {
            const isNet = /fetch|network|failed/i.test(err?.message || '') || err?.name === 'TypeError';
            setState({
                status: 'error',
                report: null,
                target: url,
                error: isNet ? '无法连接到分析服务，请确认后端已启动。' : (err.message || '未知错误')
            });
        }
    };
    const reset = () => setState({status: 'idle', report: null, error: '', target: ''});

    return {...state, run, reset, retry: () => run(lastUrl.current)};
}

/* =========================================================================
 * 6. 基础 UI 组件
 * ========================================================================= */
const ExplainBox = ({children}) => (
    <div
        className="rounded-2xl bg-white/5 border border-white/10 p-4 text-sm text-slate-300 leading-relaxed flex gap-3">
        <AlertTriangle size={16} className="text-amber-400 shrink-0 mt-0.5"/> <p>{children}</p>
    </div>
);

const SafeNote = ({text}) => (
    <div
        className="mt-4 rounded-2xl bg-emerald-500/5 border border-emerald-500/20 p-4 flex items-center gap-3 text-sm text-emerald-300">
        <CheckCircle2 size={18} className="shrink-0"/> <span>{text}</span>
    </div>
);

const StatChips = ({stats}) => !stats?.length ? null : (
    <div className="grid grid-cols-3 gap-1.5 sm:gap-2">
        {stats.map((s, i) => (
            <div key={i} className="rounded-xl bg-black/20 border border-white/5 p-2 sm:p-2.5 text-center">
                <div className="text-[10px] sm:text-xs text-slate-500 truncate">{s.label}</div>
                <div
                    className="text-xs sm:text-sm font-semibold text-slate-200 mt-0.5 tabular-nums truncate">{s.value}</div>
            </div>
        ))}
    </div>
);

const Feature = ({icon: Icon, title, desc}) => (
    <div
        className="rounded-2xl bg-white/5 border border-white/10 backdrop-blur-xl p-4 transition-colors hover:bg-white/10">
        <div className="inline-flex w-9 h-9 rounded-xl bg-emerald-500/10 items-center justify-center">
            <Icon className="text-emerald-400" size={18}/>
        </div>
        <h3 className="text-slate-200 font-medium mt-3 text-sm">{title}</h3>
        <p className="text-slate-500 text-xs mt-1 leading-relaxed">{desc}</p>
    </div>
);

/* =========================================================================
 * 7. 证据组件
 * ========================================================================= */
function MartingaleEvidence({data}) {
    const evs = data?.evidences || [];
    if (!evs.length) return <SafeNote text="未发现具体的加仓序列样本"/>;

    return (
        <div className="space-y-4 mt-4">
            <ExplainBox>「越亏越加」指行情不利时不断加大买入摊低成本。只要单边行情持续，仓位会像滚雪球般膨胀，最终可能瞬间爆仓——跟单者往往连反应时间都没有。</ExplainBox>
            {evs.slice(0, 3).map((seq, i) => {
                const first = seq[0];
                const maxQty = Math.max(...seq.map(t => t.executedQty || 0)) || 1;
                const isLong = first.positionSide === 'LONG';
                return (
                    <div key={i} className="rounded-2xl border border-white/5 bg-black/20 p-3 sm:p-4">
                        <div className="flex flex-wrap items-center gap-2 mb-4">
                            <span className="font-mono font-semibold text-slate-100">{first.symbol}</span>
                            <span
                                className={`text-xs px-2 py-0.5 rounded-md ${isLong ? 'bg-emerald-500/15 text-emerald-300' : 'bg-red-500/15 text-red-300'}`}>{isLong ? '做多' : '做空'}</span>
                            <span className="text-xs text-slate-500">连续加仓 {seq.length} 次</span>
                        </div>
                        <div
                            className="flex items-center gap-1.5 sm:gap-2 text-[11px] sm:text-xs text-slate-600 mb-1.5 px-0.5">
                            <span className="w-[72px] sm:w-20 shrink-0">时间</span>
                            <span className="w-14 sm:w-16 shrink-0 text-right">均价</span>
                            <span className="flex-1 text-right">数量</span>
                        </div>
                        <div className="space-y-1.5">
                            {seq.map((t, idx) => (
                                <div key={idx} className="flex items-center gap-1.5 sm:gap-2 text-[11px] sm:text-xs">
                                    <span
                                        className="w-[72px] sm:w-20 shrink-0 text-slate-500 tabular-nums truncate">{evTime(t)}</span>
                                    <span
                                        className="w-14 sm:w-16 shrink-0 text-right text-slate-400 tabular-nums truncate">{fmt.price(t.avgPrice)}</span>
                                    <div className="flex-1 flex items-center gap-1.5 sm:gap-2">
                                        <div className="flex-1 h-3 sm:h-4 bg-white/5 rounded-md overflow-hidden">
                                            <div className="h-full rounded-md" style={{
                                                width: `${(t.executedQty / maxQty) * 100}%`,
                                                minWidth: '6px',
                                                background: 'linear-gradient(90deg,#7f1d1d,#f87171)'
                                            }}/>
                                        </div>
                                        <span
                                            className="min-w-[40px] sm:min-w-[48px] shrink-0 text-right text-slate-200 tabular-nums font-medium truncate">{fmt.qty(t.executedQty)}</span>
                                    </div>
                                </div>
                            ))}
                        </div>
                    </div>
                );
            })}
            <p className="text-center text-xs text-slate-500 pt-2">类似的情况一共有 {data?.summary?.martingale_sequences || 0} 次</p>
        </div>
    );
}

function TailRiskEvidence({data}) {
    const ev = data?.evidences?.[0];
    const s = data?.summary || {};

    return (
        <div className="space-y-4 mt-4">
            <ExplainBox>高胜率 ≠
                稳赚。漂亮胜率的背后，往往隐藏着不对称的盈亏比——一次黑天鹅巨亏，足以吞掉数月的全部利润</ExplainBox>
            {ev && (
                <div className="rounded-2xl border border-red-500/25 bg-red-500/5 p-4">
                    <div className="flex items-center justify-between">
                        <div>
                            <div className="text-xs text-slate-400">单笔最大亏损 · <span
                                className="font-mono">{ev.symbol}</span></div>
                            <div
                                className="text-2xl sm:text-3xl font-bold text-red-400 mt-1 tabular-nums tracking-tight">{fmt.usd(ev.totalPnl)}
                                <span className="text-sm sm:text-base font-normal text-slate-500">USDT</span></div>
                        </div>
                        <Flame className="text-red-500/60 shrink-0" size={38}/>
                    </div>
                    {(ev.orderUpdateTime != null || ev.orderUpdateTime_str || ev.avgPrice != null || ev.executedQty != null) && (
                        <div
                            className="grid grid-cols-2 sm:grid-cols-3 gap-2 sm:gap-3 mt-3 pt-3 border-t border-red-500/15 text-xs">
                            <div className="col-span-2 sm:col-span-1">
                                <div className="text-slate-500">平仓时间</div>
                                <div className="text-slate-300 tabular-nums mt-0.5">{evTime(ev, true)}</div>
                            </div>
                            <div>
                                <div className="text-slate-500">成交均价</div>
                                <div className="text-slate-300 tabular-nums mt-0.5">{fmt.price(ev.avgPrice)}</div>
                            </div>
                            <div>
                                <div className="text-slate-500">成交数量</div>
                                <div className="text-slate-300 tabular-nums mt-0.5">{fmt.qty(ev.executedQty)}</div>
                            </div>
                        </div>
                    )}
                </div>
            )}
            {s.tail_risk_index != null && (
                <div
                    className="rounded-2xl bg-black/20 border border-white/5 p-4 text-sm text-slate-300 leading-relaxed">
                    这一笔亏损，需要约 <span
                    className="text-red-400 font-bold text-lg">{fmt.int(s.tail_risk_index)}</span> 笔正常盈利单才能补回来。
                </div>
            )}
        </div>
    );
}

function SlippageEvidence({data}) {
    const evs = data?.evidences || [];
    if (!evs.length) return <SafeNote text="未发现明显的超短持仓样本"/>;

    return (
        <div className="space-y-3 mt-4">
            <ExplainBox>几秒极速平仓，而因网络与撮合延迟，你的跟单往往买在更高、卖在更低，白白把利润消耗在滑点上</ExplainBox>
            {evs.slice(0, 4).map((e, i) => {
                const trades = e.sequence_trades || [];
                return (
                    <div key={i} className="rounded-2xl border border-white/5 bg-black/20 p-3 sm:p-4">
                        <div className="flex items-center justify-between mb-3">
                            <span className="font-mono text-slate-200">{trades[0]?.symbol || '—'}</span>
                            <div className="flex items-center gap-1.5 text-amber-400">
                                <Zap size={14}/><span
                                className="font-bold tabular-nums">{fmt.duration(e.hold_time_ms)}</span><span
                                className="text-xs text-slate-500 font-normal">极速平仓</span>
                            </div>
                        </div>
                        <div
                            className="flex items-center gap-1 sm:gap-2 text-[11px] sm:text-xs text-slate-600 mb-1.5 px-0.5">
                            <span className="w-9 sm:w-12 shrink-0">动作</span>
                            <span className="flex-1">时间</span>
                            <span className="w-16 sm:w-20 text-right">均价</span>
                            <span className="w-14 text-right">数量</span>
                        </div>
                        <div className="space-y-1">
                            {trades.map((t, idx) => (
                                <div key={idx}
                                     className="flex items-center gap-1 sm:gap-2 text-[11px] sm:text-xs bg-white/5 px-1.5 sm:px-2.5 py-1.5 rounded-lg">
                                    <span
                                        className={`w-9 sm:w-12 shrink-0 font-medium ${idx === 0 ? 'text-emerald-400' : idx === trades.length - 1 ? 'text-red-400' : 'text-slate-400'}`}>{idx === 0 ? '开仓' : idx === trades.length - 1 ? '平仓' : '加仓'}</span>
                                    <span
                                        className="flex-1 text-slate-400 tabular-nums truncate">{evTime(t, true)}</span>
                                    <span
                                        className="w-16 sm:w-20 text-right text-slate-300 tabular-nums truncate">{fmt.price(t.avgPrice)}</span>
                                    <span
                                        className="w-14 text-right text-slate-300 tabular-nums truncate">{fmt.qty(t.executedQty)}</span>
                                </div>
                            ))}
                        </div>
                    </div>
                );
            })}
            <p className="text-center text-xs text-slate-500 pt-2">类似的情况一共有 {data?.summary?.short_hold_sequences || 0} 次</p>
        </div>
    );
}

function HoldRatioEvidence({data}) {
    if (!data) return <SafeNote text="暂无持仓时长数据"/>;
    const {avg_profit_hold_time_ms: p, avg_loss_hold_time_ms: l, vw_hold_ratio: r} = data;
    const maxT = Math.max(p || 0, l || 0) || 1;
    const rows = [
        {label: '盈利单 · 平均持仓', val: p, bar: 'bg-emerald-500', text: 'text-emerald-400'},
        {label: '亏损单 · 平均持仓', val: l, bar: 'bg-red-500', text: 'text-red-400'},
    ];
    return (
        <div className="space-y-4 mt-4">
            <ExplainBox>健康的交易应「截断亏损、让利润奔跑」。这里正好相反：赚一点就急着落袋，亏了却长期死扛期待反转——一旦遭遇单边下跌，跟单者极易被无情套牢甚至强平</ExplainBox>
            <div className="rounded-2xl bg-black/20 border border-white/5 p-4 space-y-3">
                {rows.map(row => (
                    <div key={row.label}>
                        <div className="flex justify-between text-xs mb-1">
                            <span className={row.text}>{row.label}</span>
                            <span className="text-slate-300 tabular-nums">{fmt.duration(row.val)}</span>
                        </div>
                        <div className="h-3 rounded-full bg-white/5 overflow-hidden">
                            <div className={`h-full ${row.bar} rounded-full`}
                                 style={{width: `${((row.val || 0) / maxT) * 100}%`}}/>
                        </div>
                    </div>
                ))}
            </div>
            {r != null && <div
                className="rounded-2xl bg-black/20 border border-white/5 p-4 text-sm text-slate-300">同样一笔单，亏钱时他愿意扛 <span
                className="text-amber-400 font-bold text-lg">{fmt.num(r, 2)}</span> 倍于盈利时的时间才肯松手。</div>}
        </div>
    );
}

/* =========================================================================
 * 8. 仪表盘 & 折叠卡
 * ========================================================================= */
function RiskGauge({score, level}) {
    const meta = LEVEL_STYLES[level], val = useCountUp(score), R = 80, C = 2 * Math.PI * R,
        offset = C * (1 - val / 100);
    const gid = `gauge-${level}`;
    return (
        <div className="relative w-48 h-48 mx-auto">
            <svg viewBox="0 0 200 200" className="w-full h-full -rotate-90">
                <defs>
                    <linearGradient id={gid} x1="0" y1="0" x2="1" y2="1">
                        <stop offset="0%" stopColor={meta.hex} stopOpacity="0.55"/>
                        <stop offset="100%" stopColor={meta.hex}/>
                    </linearGradient>
                </defs>
                <circle cx="100" cy="100" r={R} fill="none" stroke="rgba(255,255,255,0.06)" strokeWidth="10"/>
                <circle cx="100" cy="100" r={R} fill="none" stroke={`url(#${gid})`} strokeWidth="10"
                        strokeLinecap="round" strokeDasharray={C} strokeDashoffset={offset}
                        style={{filter: `drop-shadow(0 0 10px ${meta.hex}66)`}}/>
            </svg>
            <div className="absolute inset-0 flex flex-col items-center justify-center">
                <span className="text-6xl font-semibold tabular-nums leading-none tracking-tight"
                      style={{color: meta.hex, textShadow: `0 0 28px ${meta.hex}55`}}>{Math.round(val)}</span>
                <span className="text-xs text-slate-500 mt-2.5 tracking-wide">风险评分 / 100</span>
            </div>
        </div>
    );
}

function DimensionCard({dim, expanded, onToggle}) {
    const meta = LEVEL_STYLES[dim.level];
    return (
        <div
            className={`rounded-2xl border ${meta.border} bg-white/5 backdrop-blur-xl overflow-hidden transition-colors`}>
            <button onClick={onToggle} className="w-full text-left p-4 md:p-5 hover:bg-white/5 transition-colors">
                <div className="flex items-center gap-3 md:gap-4">
                    <div
                        className={`shrink-0 w-11 h-11 rounded-2xl border ${meta.strip} flex items-center justify-center`}>
                        <dim.icon className={meta.text} size={22}/>
                    </div>
                    <div className="flex-1 min-w-0">
                        <div className="flex items-center gap-2 flex-wrap">
                            <h3 className="font-semibold text-slate-100 tracking-tight">{dim.title}</h3>
                            <span className={`text-xs px-2 py-0.5 rounded-full ${meta.badge}`}>{meta.label}</span>
                            <span className="text-xs text-slate-600 hidden sm:inline">· {dim.term}</span>
                        </div>
                        <p className="text-xs md:text-sm text-slate-400 mt-0.5">{dim.plain}</p>
                    </div>
                    <ChevronDown
                        className={`text-slate-500 shrink-0 transition-transform duration-300 ${expanded ? 'rotate-180' : ''}`}
                        size={20}/>
                </div>
                <div className={`mt-3.5 rounded-xl border ${meta.strip} px-4 py-3 flex items-start gap-2.5`}>
                    <meta.Icon className={`${meta.text} shrink-0 mt-0.5`} size={15}/>
                    <p className="text-xs md:text-sm text-slate-300 leading-snug">{dim.headline(meta.text)}</p>
                </div>
            </button>
            <AnimatePresence initial={false}>
                {expanded && (
                    <motion.div initial={{height: 0, opacity: 0}} animate={{height: 'auto', opacity: 1}}
                                exit={{height: 0, opacity: 0}} transition={{duration: 0.35, ease: EASE}}
                                style={{overflow: 'hidden'}}>
                        <div className="px-4 md:px-5 pb-5">
                            <div className="border-t border-white/5 pt-4">
                                <StatChips stats={dim.stats}/>
                                <dim.Component data={dim.data}/>
                            </div>
                        </div>
                    </motion.div>
                )}
            </AnimatePresence>
        </div>
    );
}

/* =========================================================================
 * 9. 视图
 * ========================================================================= */
const InputView = ({onSubmit}) => {
    const [url, setUrl] = useState('');
    const [err, setErr] = useState('');
    const submit = (e) => {
        e.preventDefault();
        url.trim() ? onSubmit(url.trim()) : setErr('请先粘贴交易员主页链接');
    };

    return (
        <div className="min-h-screen flex flex-col items-center justify-center px-5 py-12">
            <motion.div initial={{opacity: 0, y: 18}} animate={{opacity: 1, y: 0}}
                        transition={{duration: 0.7, ease: EASE}} className="w-full max-w-xl text-center">
                <div
                    className="inline-flex items-center justify-center w-16 h-16 rounded-3xl bg-gradient-to-b from-white/10 to-white/5 border border-white/10 backdrop-blur-xl mb-7 shadow-2xl shadow-emerald-500/10">
                    <Radar className="text-emerald-400" size={30}/>
                </div>
                <h1 className="text-4xl md:text-5xl font-semibold tracking-tight bg-gradient-to-b from-white to-slate-400 bg-clip-text text-transparent">跟单风险透视镜</h1>
                <p className="text-slate-400 mt-4 md:text-lg leading-relaxed">粘贴币安带单员链接，透视漂亮收益率背后<span
                    className="text-red-400 font-medium">真正会让你亏钱</span>的操作习惯。</p>

                <form onSubmit={submit} className="mt-9">
                    <div className="relative group">
                        <Link2
                            className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500 group-focus-within:text-emerald-400 transition-colors"
                            size={18}/>
                        <input value={url} onChange={(e) => {
                            setUrl(e.target.value);
                            setErr('');
                        }} placeholder="粘贴带单员主页链接…"
                               className="w-full pl-12 pr-4 py-4 rounded-2xl bg-white/5 border border-white/10 backdrop-blur-xl text-slate-100 placeholder-slate-600 text-sm focus:outline-none focus:border-emerald-500/50 focus:bg-white/10 focus:ring-4 focus:ring-emerald-500/10 transition-all"/>
                    </div>
                    {err && <p className="text-red-400 text-xs mt-2.5 text-left px-1">{err}</p>}
                    <motion.button whileHover={{scale: 1.01}} whileTap={{scale: 0.985}} type="submit"
                                   className="w-full mt-3.5 py-4 rounded-2xl bg-gradient-to-b from-emerald-400 to-emerald-500 hover:from-emerald-300 hover:to-emerald-400 text-slate-950 font-semibold shadow-lg shadow-emerald-500/25 transition-colors flex items-center justify-center gap-2">
                        <Search size={18}/> 一键透视风险
                    </motion.button>
                </form>

                <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-12 text-left">
                    <Feature icon={Layers} title="警惕越亏越加" desc="看清赌徒式加仓的爆仓隐患"/>
                    <Feature icon={TrendingDown} title="深挖赢小亏大" desc="高胜率背后往往藏着致命巨亏"/>
                    <Feature icon={Clock} title="还原真实持仓" desc="识别闪电平仓与死扛坏习惯"/>
                </div>
            </motion.div>
        </div>
    );
};

const LoadingView = () => {
    const [step, setStep] = useState(0);
    useEffect(() => {
        const t = setInterval(() => setStep(s => (s + 1) % LOADING_STEPS.length), 800);
        return () => clearInterval(t);
    }, []);
    return (
        <div className="min-h-screen flex flex-col items-center justify-center px-4">
            <div className="relative w-28 h-28 flex items-center justify-center">
                <motion.div className="absolute inset-0 rounded-full bg-emerald-500/10 blur-2xl"
                            animate={{scale: [1, 1.2, 1], opacity: [0.35, 0.7, 0.35]}}
                            transition={{duration: 2, repeat: Infinity, ease: 'easeInOut'}}/>
                <motion.div className="absolute inset-2 rounded-full border border-emerald-500/15"
                            animate={{scale: [1, 1.15, 1], opacity: [0.6, 0.2, 0.6]}}
                            transition={{duration: 1.6, repeat: Infinity}}/>
                <motion.div className="absolute inset-2 rounded-full border-t-2 border-emerald-400"
                            animate={{rotate: 360}} transition={{duration: 1.2, repeat: Infinity, ease: 'linear'}}/>
                <Radar className="text-emerald-400 relative" size={34}/>
            </div>
            <h2 className="text-slate-200 font-medium mt-8 tracking-tight">正在透视交易风险</h2>
            <AnimatePresence mode="wait">
                <motion.p key={step} initial={{opacity: 0, y: 6}} animate={{opacity: 1, y: 0}}
                          exit={{opacity: 0, y: -6}}
                          className="text-sm text-slate-500 mt-2.5 h-5">{LOADING_STEPS[step]}</motion.p>
            </AnimatePresence>
        </div>
    );
};

const ErrorView = ({message, onRetry, onReset}) => (
    <div className="min-h-screen flex flex-col items-center justify-center px-4 text-center">
        <motion.div initial={{opacity: 0, scale: 0.9}} animate={{opacity: 1, scale: 1}}
                    transition={{duration: 0.5, ease: EASE}} className="flex flex-col items-center">
            <div
                className="inline-flex items-center justify-center w-16 h-16 rounded-3xl bg-red-500/10 border border-red-500/25 backdrop-blur-xl mb-5">
                <XCircle className="text-red-400" size={32}/></div>
            <h2 className="text-xl font-semibold text-slate-100 tracking-tight">检测失败</h2>
            <p className="text-sm text-slate-400 mt-2 max-w-sm leading-relaxed">{message}</p>
            <div className="flex gap-3 mt-6">
                <motion.button whileTap={{scale: 0.97}} onClick={onRetry}
                               className="px-5 py-2.5 rounded-2xl bg-gradient-to-b from-emerald-400 to-emerald-500 text-slate-950 font-medium shadow-lg shadow-emerald-500/20 transition">重试
                </motion.button>
                <motion.button whileTap={{scale: 0.97}} onClick={onReset}
                               className="px-5 py-2.5 rounded-2xl bg-white/5 border border-white/10 hover:bg-white/10 text-slate-200 transition">返回
                </motion.button>
            </div>
        </motion.div>
    </div>
);

const ReportView = ({report, target, onReset}) => {
    const dims = useMemo(() => buildDimensions(report), [report]);
    const ov = report?.overview || {};
    const riskScore = ov.risk_score || 0;
    const level = getLevel(riskScore);
    const verdict = OVERVIEW[level];
    const highCount = dims.filter(d => d.level === 'high').length;
    const medCount = dims.filter(d => d.level === 'medium').length;
    const [openKey, setOpenKey] = useState(null);

    const isEmpty = !report || Object.keys(report).length === 0;
    const isZero = !isEmpty && ov.total_trades === 0;

    // 计算天数跨度和直观的日期展示
    let daysSpan = '—';
    let dateStr = '—';
    if (ov.start_time && ov.end_time) {
        const diff = new Date(ov.end_time).getTime() - new Date(ov.start_time).getTime();
        daysSpan = Math.max(1, Math.ceil(diff / 86400000));
        dateStr = `${fmt.date(ov.start_time).split(' ')[0]} 至 ${fmt.date(ov.end_time).split(' ')[0]}`;
    }

    return (
        <div className="max-w-3xl mx-auto px-4 py-6 md:py-10">
            <button onClick={onReset}
                    className="flex items-center gap-1.5 text-sm text-slate-400 hover:text-slate-200 mb-5 transition-colors">
                <ArrowLeft size={16}/> 重新检测
            </button>
            {target && <div className="text-xs text-slate-500 mb-4 truncate">分析目标：{target}</div>}

            {isEmpty || isZero ? (
                <div className="rounded-3xl border border-white/10 bg-white/5 backdrop-blur-xl p-10 text-center">
                    {isEmpty ? <XCircle className="mx-auto text-slate-500 mb-4" size={48}/> :
                        <Search className="mx-auto text-emerald-400 mb-4" size={48}/>}
                    <h3 className="text-lg font-semibold text-slate-200 tracking-tight">{isEmpty ? '未获取到有效数据' : '该交易员暂无交易记录'}</h3>
                    <p className="text-sm text-slate-500 mt-2">{isEmpty ? '请确认链接正确，或稍后重试。' : '无法基于空白记录进行风险评估。'}</p>
                </div>
            ) : (
                <>
                    <motion.div initial={{opacity: 0, y: 16, scale: 0.98}} animate={{opacity: 1, y: 0, scale: 1}}
                                transition={{duration: 0.55, ease: EASE}}
                                className="relative rounded-3xl border bg-gradient-to-b from-white/10 to-white/5 backdrop-blur-2xl p-6 md:p-9 mb-5 text-center overflow-hidden"
                                style={{borderColor: LEVEL_STYLES[level].hex + '30'}}>
                        <div
                            className="absolute -top-24 left-1/2 -translate-x-1/2 w-72 h-72 rounded-full pointer-events-none"
                            style={{background: LEVEL_STYLES[level].hex, opacity: 0.12, filter: 'blur(80px)'}}/>
                        <div className="relative">
                            <RiskGauge score={riskScore} level={level}/>
                            <div className="mt-5">
                                <h2 className="text-2xl md:text-3xl font-semibold tracking-tight" style={{
                                    color: LEVEL_STYLES[level].hex,
                                    textShadow: `0 0 30px ${LEVEL_STYLES[level].hex}40`
                                }}>{verdict.word}</h2>
                            </div>

                            <div className="mt-6 flex flex-wrap items-center justify-center gap-3">
                                <div
                                    className="flex items-center gap-2.5 bg-black/20 border border-white/10 rounded-2xl px-4 py-2.5">
                                    <span className="text-slate-500 text-sm">交易笔数</span>
                                    <span
                                        className="text-slate-200 font-semibold tabular-nums">{fmt.int(ov.total_trades)}
                                        <span className="text-xs font-normal text-slate-500">笔</span></span>
                                </div>
                                <div
                                    className="flex flex-col sm:flex-row items-center gap-1 sm:gap-2.5 bg-black/20 border border-white/10 rounded-2xl px-4 py-2.5">
                                    <div className="flex items-center gap-2.5">
                                        <span className="text-slate-500 text-sm">所分析交易跨度</span>
                                        <span className="text-slate-200 font-semibold tabular-nums">{daysSpan} <span
                                            className="text-xs font-normal text-slate-500">天</span></span>
                                    </div>
                                    <div className="hidden sm:block w-px h-3 bg-white/10 mx-1"/>
                                    <span className="text-slate-400 text-xs tabular-nums">{dateStr}</span>
                                </div>
                            </div>
                        </div>
                    </motion.div>

                    {dims.length === 0 ? (
                        <div
                            className="rounded-3xl border border-white/10 bg-white/5 backdrop-blur-xl p-10 text-center">
                            <ShieldCheck className="mx-auto text-emerald-400 mb-4" size={48}/><h3
                            className="text-slate-200 font-semibold tracking-tight">未检测到可分析的风险维度</h3></div>
                    ) : (
                        <>
                            <div className="flex items-center gap-2 px-1 mb-3 mt-6">
                                <Flame className="text-slate-500" size={16}/>
                                <h3 className="text-sm font-medium text-slate-300">{highCount + medCount > 0 ? '风险明细' : '各维度数据'}</h3>
                                <span className="text-xs text-slate-600">点击卡片展开原始成交记录</span>
                            </div>
                            <div className="space-y-3">
                                {dims.map((d, i) => (
                                    <motion.div key={d.key} initial={{opacity: 0, y: 12}} animate={{opacity: 1, y: 0}}
                                                transition={{duration: 0.4, delay: i * 0.06, ease: EASE}}>
                                        <DimensionCard dim={d} expanded={openKey === d.key}
                                                       onToggle={() => setOpenKey(openKey === d.key ? null : d.key)}/>
                                    </motion.div>
                                ))}
                            </div>
                        </>
                    )}
                </>
            )}
            <p className="text-xs text-slate-600 text-center mt-10 max-w-lg mx-auto leading-relaxed">本报告基于历史公开成交数据分析，仅供参考，不构成任何投资建议。加密资产交易风险极高，过往表现不代表未来收益，请独立决策。</p>
        </div>
    );
};

/* =========================================================================
 * 10. 根组件
 * ========================================================================= */
export default function App() {
    const {status, report, error, target, run, reset, retry} = useRiskAnalysis();
    return (
        <div className="relative min-h-screen text-slate-100 antialiased selection:bg-emerald-500/30 overflow-x-hidden"
             style={{background: '#060709', fontFamily: FONT_STACK}}>
            {/* 环境光晕 */}
            <div className="pointer-events-none fixed inset-0 overflow-hidden">
                <div className="absolute left-1/2 -translate-x-1/2 -top-40 w-96 h-96 rounded-full" style={{
                    background: 'radial-gradient(circle, rgba(16,185,129,0.16), transparent 70%)',
                    filter: 'blur(50px)'
                }}/>
                <div className="absolute right-0 top-1/3 w-80 h-80 rounded-full" style={{
                    background: 'radial-gradient(circle, rgba(59,130,246,0.10), transparent 70%)',
                    filter: 'blur(70px)'
                }}/>
                <div className="absolute left-0 bottom-0 w-80 h-80 rounded-full" style={{
                    background: 'radial-gradient(circle, rgba(139,92,246,0.08), transparent 70%)',
                    filter: 'blur(70px)'
                }}/>
            </div>
            {/* 细腻噪点质感 */}
            <div className="pointer-events-none fixed inset-0" style={{backgroundImage: GRAIN, opacity: 0.05}}/>
            <div className="relative">
                {status === 'idle' && <InputView onSubmit={run}/>}
                {status === 'loading' && <LoadingView/>}
                {status === 'error' && <ErrorView message={error} onRetry={retry} onReset={reset}/>}
                {status === 'success' && <ReportView report={report} target={target} onReset={reset}/>}
            </div>
        </div>
    );
}