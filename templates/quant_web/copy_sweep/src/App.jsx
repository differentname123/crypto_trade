import React, { useState, useEffect, useMemo, useRef } from 'react';
import { motion, AnimatePresence } from 'framer-motion';
import {
  Radar, Search, Link2, Layers, TrendingDown, Clock, Zap,
  ShieldAlert, ShieldCheck, AlertTriangle, CheckCircle2,
  ChevronDown, ArrowLeft, XCircle,
} from 'lucide-react';

/* =========================================================================
 * 1. 核心配置 & 样式字典
 * ========================================================================= */
const LEVEL_STYLES = {
  high: { label: '高危', hex: '#ef4444', Icon: ShieldAlert, border: 'border-red-500/40', iconBg: 'bg-red-500/10', text: 'text-red-400', badge: 'bg-red-500/15 text-red-300' },
  medium: { label: '警示', hex: '#f59e0b', Icon: AlertTriangle, border: 'border-amber-500/40', iconBg: 'bg-amber-500/10', text: 'text-amber-400', badge: 'bg-amber-500/15 text-amber-300' },
  low: { label: '安全', hex: '#10b981', Icon: ShieldCheck, border: 'border-emerald-500/40', iconBg: 'bg-emerald-500/10', text: 'text-emerald-400', badge: 'bg-emerald-500/15 text-emerald-300' },
};

const LOADING_STEPS = ['正在拉取历史成交记录…', '识别加仓与马丁格尔序列…', '计算尾部风险指数…', '还原真实持仓与盈亏结构…', '生成风险评估报告…'];

/* =========================================================================
 * 2. 工具类 (格式化 & 算法)
 * ========================================================================= */
const fmt = {
  num: (n, d = 2) => n == null ? '—' : Number(n).toLocaleString('en-US', { maximumFractionDigits: d }),
  pct: (n, d = 1) => n == null ? '—' : `${Number(n).toFixed(d)}%`,
  money: (n) => n == null ? '—' : Math.abs(n) >= 1 ? n.toFixed(2) : n.toFixed(4),
  date: (ts) => ts ? new Intl.DateTimeFormat('zh-CN', { year: 'numeric', month: '2-digit', day: '2-digit', hour: '2-digit', minute: '2-digit', hour12: false }).format(new Date(ts)).replace(/\//g, '-') : '—',
  price: (n) => {
    if (n == null) return '—';
    const abs = Math.abs(n);
    const s = abs >= 100 ? n.toFixed(2) : abs >= 1 ? n.toFixed(4) : n.toFixed(6);
    return s.replace(/(\.\d*?)0+$/, '$1').replace(/\.$/, '');
  },
  qty: (n) => n == null ? '—' : n >= 1 ? n.toLocaleString('en-US', { maximumFractionDigits: 2 }) : String(n),
  duration: (ms) => {
    if (ms == null) return '—';
    const thresholds = [[86400000, '天'], [3600000, '小时'], [60000, '分钟'], [1000, '秒']];
    for (let [t, label] of thresholds) if (ms >= t) return `${(ms / t).toFixed(1)} ${label}`;
    return `${Math.round(ms)} 毫秒`;
  }
};

const calcScore = (v, [lowMax, highMin, maxVal]) => {
  if (v == null) return 0;
  let s = v <= lowMax ? (v / lowMax) * 33 : v <= highMin ? 33 + ((v - lowMax) / (highMin - lowMax)) * 33 : 66 + ((v - highMin) / (maxVal - highMin)) * 34;
  return Math.max(0, Math.min(100, s));
};
const getLevel = (s) => (s >= 67 ? 'high' : s >= 34 ? 'medium' : 'low');

// 数字滚动 Hook
function useCountUp(target, duration = 1400) {
  const [val, setVal] = useState(0);
  useEffect(() => {
    let raf, start = performance.now();
    const tick = (now) => {
      const p = Math.min(1, (now - start) / duration);
      setVal(target * (1 - Math.pow(1 - p, 3))); // easeOutCubic
      p < 1 ? raf = requestAnimationFrame(tick) : setVal(target);
    };
    raf = requestAnimationFrame(tick);
    return () => cancelAnimationFrame(raf);
  }, [target, duration]);
  return val;
}

/* =========================================================================
 * 3. 维度配置字典 (配置驱动：修改或新增维度只需动这里)
 * ========================================================================= */
const RISK_CONFIG = [
  {
    key: 'martingale', title: '马丁格尔加仓', icon: Layers,
    plain: '越亏越加码，风险指数级放大',
    extractValue: (d) => d?.martingale?.summary?.martingale_rate_percent,
    thresholds: [10, 25, 60],
    buildNode: (d, score) => ({
      displayValue: fmt.pct(d.martingale.summary.martingale_rate_percent), metricLabel: '马丁占比',
      stats: [
        { label: '加仓序列', value: fmt.num(d.martingale.summary.total_add_sequences, 0) },
        { label: '马丁序列', value: fmt.num(d.martingale.summary.martingale_sequences, 0) },
        { label: '最小加仓', value: d.martingale.summary.min_actual_multiplier != null ? `×${d.martingale.summary.min_actual_multiplier}` : '—' },
      ],
      Component: MartingaleEvidence, data: d.martingale
    })
  },
  {
    key: 'tail_risk', title: '尾部风险', icon: TrendingDown,
    plain: '高胜率的背后，藏着致命的单笔巨亏',
    extractValue: (d) => d?.tail_risk?.summary?.tail_risk_index,
    thresholds: [20, 50, 150],
    buildNode: (d, score) => ({
      displayValue: fmt.num(d.tail_risk.summary.tail_risk_index, 0), metricLabel: '尾部风险指数',
      stats: [
        { label: '最大亏损', value: d.tail_risk.summary.max_single_loss != null ? `-${fmt.num(d.tail_risk.summary.max_single_loss, 2)}` : '—' },
        { label: '中位盈利', value: fmt.num(d.tail_risk.summary.median_single_profit, 3) },
        { label: '盈亏笔数', value: `${fmt.num(d.tail_risk.summary.win_count, 0)}/${fmt.num(d.tail_risk.summary.loss_count, 0)}` },
      ],
      Component: TailRiskEvidence, data: d.tail_risk
    })
  },
  {
    key: 'slippage_trap', title: '滑点陷阱', icon: Zap,
    plain: '数秒极速平仓，跟单者高买低卖被割',
    extractValue: (d) => d?.slippage_trap?.summary?.slippage_trap_ratio_percent,
    thresholds: [8, 18, 40],
    buildNode: (d, score) => ({
      displayValue: fmt.pct(d.slippage_trap.summary.slippage_trap_ratio_percent), metricLabel: '超短持仓占比',
      stats: [
        { label: '总序列', value: fmt.num(d.slippage_trap.summary.total_sequences, 0) },
        { label: '超短持仓', value: fmt.num(d.slippage_trap.summary.short_hold_sequences, 0) },
        { label: '平均持仓', value: d.slippage_trap.summary.average_hold_time_minutes != null ? `${fmt.num(d.slippage_trap.summary.average_hold_time_minutes, 0)}分` : '—' },
      ],
      Component: SlippageEvidence, data: d.slippage_trap
    })
  },
  {
    key: 'vw_hold_ratio', title: '持仓纪律', icon: Clock,
    plain: '赚小钱就跑，亏大钱死扛，纪律缺失',
    extractValue: (d) => d?.vw_hold_ratio?.vw_hold_ratio,
    thresholds: [1.1, 1.5, 3],
    buildNode: (d, score) => ({
      displayValue: `×${fmt.num(d.vw_hold_ratio.vw_hold_ratio, 2)}`, metricLabel: '亏损/盈利持仓比',
      stats: [], Component: HoldRatioEvidence, data: d.vw_hold_ratio
    })
  }
];

// 核心提取逻辑：遍历配置生成维度数组
const buildDimensions = (data) => !data ? [] : RISK_CONFIG.reduce((acc, cfg) => {
  const val = cfg.extractValue(data);
  if (val != null) {
    const score = calcScore(val, cfg.thresholds);
    acc.push({ ...cfg, score, level: getLevel(score), ...cfg.buildNode(data, score) });
  }
  return acc;
}, []);

/* =========================================================================
 * 4. API & 状态管理 Hook (分离业务逻辑与 UI)
 * ========================================================================= */
function useRiskAnalysis() {
  const [state, setState] = useState({ status: 'idle', report: null, error: '', target: '' });
  const lastUrl = useRef('');

  const run = async (url) => {
    lastUrl.current = url;
    setState({ status: 'loading', report: null, error: '', target: url });
    try {
      const res = await fetch('http://localhost:8000/api/report', {
        method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify({ url }),
      });
      if (!res.ok) throw new Error(`服务器返回错误状态码：${res.status}`);
      setState({ status: 'success', report: await res.json(), error: '', target: url });
    } catch (err) {
      const isNet = /fetch|network|failed/i.test(err?.message || '') || err?.name === 'TypeError';
      setState({ status: 'error', report: null, target: url, error: isNet ? '无法连接到分析服务，请确认后端已启动。' : (err.message || '未知错误') });
    }
  };
  const reset = () => setState({ status: 'idle', report: null, error: '', target: '' });

  return { ...state, run, reset, retry: () => run(lastUrl.current) };
}

/* =========================================================================
 * 5. 基础 UI 组件
 * ========================================================================= */
const ExplainBox = ({ children }) => (
  <div className="rounded-xl bg-slate-800/40 border border-slate-700/50 p-3.5 text-sm text-slate-300 leading-relaxed flex gap-2.5">
    <AlertTriangle size={16} className="text-amber-400 shrink-0 mt-0.5" /> <p>{children}</p>
  </div>
);

const SafeNote = ({ text }) => (
  <div className="mt-4 rounded-xl bg-emerald-500/5 border border-emerald-500/20 p-4 flex items-center gap-3 text-sm text-emerald-300">
    <CheckCircle2 size={18} className="shrink-0" /> <span>{text}</span>
  </div>
);

const StatChips = ({ stats }) => !stats?.length ? null : (
  <div className="grid grid-cols-3 gap-2 mt-4">
    {stats.map((s, i) => (
      <div key={i} className="rounded-lg bg-slate-950/50 border border-slate-800 p-2.5 text-center">
        <div className="text-xs text-slate-500 truncate">{s.label}</div>
        <div className="text-sm font-semibold text-slate-200 mt-0.5">{s.value}</div>
      </div>
    ))}
  </div>
);

const Feature = ({ icon: Icon, title, desc }) => (
  <div className="rounded-xl bg-slate-900/50 border border-slate-800 p-4">
    <Icon className="text-emerald-400" size={20} />
    <h3 className="text-slate-200 font-medium mt-2 text-sm">{title}</h3>
    <p className="text-slate-500 text-xs mt-1">{desc}</p>
  </div>
);

/* =========================================================================
 * 6. 业务证据组件 (Evidences)
 * ========================================================================= */
function MartingaleEvidence({ data }) {
  const evs = data?.evidences || [];
  if (!evs.length) return <SafeNote text="未发现具体的马丁格尔加仓序列样本" />;

  return (
    <div className="space-y-4 mt-4">
      <ExplainBox>马丁格尔策略在亏损时不断加大买入。一旦持续单边下跌，仓位呈指数级膨胀，极易爆仓。</ExplainBox>
      {evs.slice(0, 3).map((seq, i) => {
        const first = seq[0], last = seq[seq.length - 1];
        const priceChg = first.avgPrice ? ((last.avgPrice - first.avgPrice) / first.avgPrice) * 100 : 0;
        const totalQty = seq.reduce((sum, t) => sum + (t.executedQty || 0), 0);
        const maxQty = Math.max(...seq.map(t => t.executedQty || 0)) || 1;

        return (
          <div key={i} className="rounded-xl border border-slate-800 bg-slate-950/50 p-4">
            <div className="flex flex-wrap items-center gap-2 mb-3 text-xs">
              <span className="font-mono font-semibold text-slate-100 text-sm">{first.symbol}</span>
              <span className="px-2 py-0.5 rounded bg-slate-800 text-slate-300">{first.positionSide === 'LONG' ? '做多' : '做空'}</span>
              <span className="text-slate-500">连续加仓 {seq.length} 次</span>
            </div>
            <div className="grid grid-cols-2 gap-2 mb-4 text-xs">
              <div className="rounded-lg bg-slate-900 p-2.5">
                <div className="text-slate-500">均价变化</div>
                <div className="text-slate-200 mt-0.5">{fmt.price(first.avgPrice)} → {fmt.price(last.avgPrice)} <span className="text-red-400">({priceChg > 0 ? '+' : ''}{priceChg.toFixed(1)}%)</span></div>
              </div>
              <div className="rounded-lg bg-slate-900 p-2.5">
                <div className="text-slate-500">累计仓位放大</div>
                <div className="text-slate-200 mt-0.5">{fmt.qty(first.executedQty)} → {fmt.qty(totalQty)} <span className="text-red-400">(×{(totalQty/first.executedQty).toFixed(1)})</span></div>
              </div>
            </div>
            <div className="space-y-1.5">
              {seq.map((t, idx) => (
                <div key={idx} className="flex items-center gap-2 text-xs">
                  <span className="w-14 shrink-0 text-slate-500 tabular-nums">{t.orderUpdateTime_str ? t.orderUpdateTime_str.split(' ')[1]?.slice(0,5) : '—'}</span>
                  <span className="w-14 shrink-0 text-right text-slate-400 tabular-nums">{fmt.price(t.avgPrice)}</span>
                  <div className="flex-1 h-4 bg-slate-800/70 rounded overflow-hidden">
                    <div className="h-full rounded" style={{ width: `${(t.executedQty / maxQty) * 100}%`, minWidth: '6px', background: 'linear-gradient(90deg,#991b1b,#f87171)' }} />
                  </div>
                  <span className="w-14 shrink-0 text-right text-slate-200 tabular-nums font-medium">{fmt.qty(t.executedQty)}</span>
                </div>
              ))}
            </div>
          </div>
        );
      })}
      {evs.length > 3 && <div className="text-center text-xs text-slate-500 pt-1">另有 {evs.length - 3} 个序列未展示</div>}
    </div>
  );
}

function TailRiskEvidence({ data }) {
  const ev = data?.evidences?.[0];
  const { tail_risk_index: idx, median_single_profit, win_count, loss_count } = data?.summary || {};
  const winRate = (win_count + loss_count) > 0 ? (win_count / (win_count + loss_count)) * 100 : null;

  return (
    <div className="space-y-4 mt-4">
      <ExplainBox>靠大量小额盈利堆出漂亮胜率，但盈亏极不对称。一次黑天鹅巨亏足以吞掉数月利润。</ExplainBox>
      {ev && (
        <div className="rounded-xl border border-red-500/30 bg-red-500/5 p-4 flex items-center justify-between">
          <div>
            <div className="text-xs text-slate-400">单笔最大亏损 · {ev.symbol}</div>
            <div className="text-3xl font-bold text-red-400 mt-1">{fmt.money(ev.totalPnl)} <span className="text-base font-normal text-slate-500">USDT</span></div>
          </div>
          <TrendingDown className="text-red-500/70 shrink-0" size={40} />
        </div>
      )}
      {idx != null && median_single_profit != null && (
        <div className="rounded-xl bg-slate-950/50 border border-slate-800 p-4 text-sm text-slate-300">
          这一笔亏损，需要约 <span className="text-red-400 font-bold text-lg">{Math.round(idx)}</span> 笔盈利单才能填平。
        </div>
      )}
      {winRate != null && (
        <div className="rounded-xl bg-slate-950/50 border border-slate-800 p-4">
          <div className="flex justify-between text-xs mb-2"><span className="text-emerald-400">盈利 {win_count} 笔</span><span className="text-red-400">亏损 {loss_count} 笔</span></div>
          <div className="h-2.5 rounded-full overflow-hidden bg-slate-800 flex">
            <div className="h-full bg-emerald-500" style={{ width: `${winRate}%` }} />
            <div className="h-full bg-red-500" style={{ width: `${100 - winRate}%` }} />
          </div>
        </div>
      )}
    </div>
  );
}

function SlippageEvidence({ data }) {
  const evs = data?.evidences || [];
  if (!evs.length) return <SafeNote text="未发现明显的超短持仓滑点样本" />;

  return (
    <div className="space-y-3 mt-4">
      <ExplainBox>数秒极速平仓，跟单者由于网络延迟往往买在更高点、卖在更低点，沦为被收割的对象。</ExplainBox>
      {evs.slice(0, 4).map((e, i) => (
        <div key={i} className="rounded-xl border border-slate-800 bg-slate-950/50 p-4 flex items-center gap-4">
          <div className="text-center shrink-0">
            <div className="text-2xl font-bold text-amber-400">{fmt.duration(e.hold_time_ms)}</div>
            <div className="text-xs text-slate-500">持仓时长</div>
          </div>
          <div className="flex-1 min-w-0 space-y-1.5">
            <div className="font-mono text-slate-200 mb-2">{e.sequence_trades?.[0]?.symbol || '—'}</div>
            {e.sequence_trades?.map((t, idx) => (
              <div key={idx} className="flex items-center gap-3 text-xs bg-slate-900/40 px-2.5 py-1.5 rounded">
                <span className="text-slate-500 tabular-nums shrink-0">{t.orderUpdateTime ? fmt.date(t.orderUpdateTime).slice(11,19) : t.orderUpdateTime_str?.slice(11,19) || '—'}</span>
                <span className="text-slate-300 tabular-nums flex-1">{fmt.price(t.avgPrice)}</span>
                <span className="text-slate-400 tabular-nums shrink-0">{fmt.qty(t.executedQty)}</span>
              </div>
            ))}
          </div>
        </div>
      ))}
    </div>
  );
}

function HoldRatioEvidence({ data }) {
  if (!data) return <SafeNote text="暂无持仓时长数据" />;
  const { avg_profit_hold_time_ms: p, avg_loss_hold_time_ms: l, vw_hold_ratio: r } = data;
  const maxT = Math.max(p || 0, l || 0) || 1;

  return (
    <div className="space-y-4 mt-4">
      <ExplainBox>盈利早早获利了结，亏损却长期死扛。纪律缺失的典型信号。</ExplainBox>
      <div className="space-y-3">
        {[{ label: '盈利单平均持仓', val: p, color: 'emerald' }, { label: '亏损单平均持仓', val: l, color: 'red' }].map(item => (
          <div key={item.label}>
            <div className={`flex justify-between text-xs mb-1 text-${item.color}-400`}>
              <span>{item.label}</span><span className="text-slate-300">{fmt.duration(item.val)}</span>
            </div>
            <div className="h-3 rounded-full bg-slate-800 overflow-hidden">
              <div className={`h-full bg-${item.color}-500 rounded-full`} style={{ width: `${(item.val / maxT) * 100}%` }} />
            </div>
          </div>
        ))}
      </div>
      {r != null && <div className="rounded-xl bg-slate-950/50 border border-slate-800 p-4 text-sm text-slate-300">亏损持仓时间是盈利的 <span className="text-amber-400 font-bold text-lg">{r.toFixed(2)}</span> 倍。</div>}
    </div>
  );
}

/* =========================================================================
 * 7. 高级 UI 块 (仪表盘 & 折叠卡片)
 * ========================================================================= */
function RiskGauge({ score, level }) {
  const meta = LEVEL_STYLES[level], val = useCountUp(score), R = 80, C = 2 * Math.PI * R, offset = C * (1 - val / 100);
  return (
    <div className="relative w-52 h-52 mx-auto">
      <svg viewBox="0 0 200 200" className="w-full h-full -rotate-90">
        <circle cx="100" cy="100" r={R} fill="none" stroke="#1e293b" strokeWidth="13" />
        <circle cx="100" cy="100" r={R} fill="none" stroke={meta.hex} strokeWidth="13" strokeLinecap="round" strokeDasharray={C} strokeDashoffset={offset} style={{ filter: `drop-shadow(0 0 6px ${meta.hex}80)` }} />
      </svg>
      <div className="absolute inset-0 flex flex-col items-center justify-center">
        <span className="text-6xl font-bold tabular-nums" style={{ color: meta.hex }}>{Math.round(val)}</span>
        <span className="text-xs text-slate-500 mt-1">风险指数 / 100</span>
      </div>
    </div>
  );
}

function DimensionCard({ dim, expanded, onToggle }) {
  const meta = LEVEL_STYLES[dim.level];
  return (
    <div className={`rounded-2xl border ${meta.border} bg-slate-900/60 overflow-hidden transition-colors`}>
      <button onClick={onToggle} className="w-full flex items-center gap-3 md:gap-4 p-4 md:p-5 text-left hover:bg-slate-800/30">
        <div className={`shrink-0 w-11 h-11 rounded-xl ${meta.iconBg} flex items-center justify-center`}><dim.icon className={meta.text} size={22} /></div>
        <div className="flex-1 min-w-0">
          <div className="flex items-center gap-2 flex-wrap"><h3 className="font-semibold text-slate-100">{dim.title}</h3><span className={`text-xs px-2 py-0.5 rounded-full ${meta.badge}`}>{meta.label}</span></div>
          <p className="text-xs md:text-sm text-slate-400 mt-0.5 truncate">{dim.plain}</p>
        </div>
        <div className="text-right shrink-0">
          <div className="text-base md:text-lg font-bold" style={{ color: meta.hex }}>{dim.displayValue}</div>
          <div className="text-xs text-slate-500">{dim.metricLabel}</div>
        </div>
        <ChevronDown className={`text-slate-500 shrink-0 transition-transform duration-300 ${expanded ? 'rotate-180' : ''}`} size={20} />
      </button>
      <AnimatePresence initial={false}>
        {expanded && (
          <motion.div initial={{ height: 0, opacity: 0 }} animate={{ height: 'auto', opacity: 1 }} exit={{ height: 0, opacity: 0 }} transition={{ duration: 0.3 }} style={{ overflow: 'hidden' }}>
            <div className="px-4 md:px-5 pb-5 border-t border-slate-800/70">
              <StatChips stats={dim.stats} />
              <dim.Component data={dim.data} />
            </div>
          </motion.div>
        )}
      </AnimatePresence>
    </div>
  );
}

/* =========================================================================
 * 8. 视图层 (Views)
 * ========================================================================= */
const InputView = ({ onSubmit }) => {
  const [url, setUrl] = useState('');
  const [err, setErr] = useState('');

  const submit = (e) => { e.preventDefault(); url.trim() ? onSubmit(url.trim()) : setErr('请输入链接'); };

  return (
    <div className="min-h-screen flex flex-col items-center justify-center px-4 py-10">
      <div className="w-full max-w-xl text-center">
        <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-slate-800/60 border border-slate-700 mb-6"><Radar className="text-emerald-400" size={30} /></div>
        <h1 className="text-3xl md:text-4xl font-bold text-slate-100">跟单风险透视镜</h1>
        <p className="text-slate-400 mt-3 md:text-lg leading-relaxed">输入币安带单交易员链接，一键看穿漂亮收益率背后的<span className="text-red-400 font-medium">真实风险</span>。</p>

        <form onSubmit={submit} className="mt-8">
          <div className="relative">
            <Link2 className="absolute left-4 top-1/2 -translate-y-1/2 text-slate-500" size={18} />
            <input value={url} onChange={(e) => { setUrl(e.target.value); setErr(''); }} placeholder="粘贴带单员主页链接..." className="w-full pl-11 pr-4 py-3.5 rounded-xl bg-slate-900 border border-slate-700 text-slate-100 placeholder-slate-600 text-sm focus:outline-none focus:border-emerald-500/60 focus:ring-2 transition" />
          </div>
          {err && <p className="text-red-400 text-xs mt-2 text-left">{err}</p>}
          <button type="submit" className="w-full mt-3 py-3.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-semibold transition flex items-center justify-center gap-2"><Search size={18} /> 开始检测</button>
        </form>

        <div className="grid grid-cols-1 sm:grid-cols-3 gap-3 mt-10 text-left">
          <Feature icon={Layers} title="识别马丁加仓" desc="揭示越亏越加的隐患" />
          <Feature icon={TrendingDown} title="透视尾部风险" desc="看穿高胜率背后的巨亏" />
          <Feature icon={Clock} title="还原真实持仓" desc="识别滑点与纪律缺失" />
        </div>
      </div>
    </div>
  );
};

const LoadingView = () => {
  const [step, setStep] = useState(0);
  useEffect(() => { const t = setInterval(() => setStep(s => (s + 1) % LOADING_STEPS.length), 800); return () => clearInterval(t); }, []);
  return (
    <div className="min-h-screen flex flex-col items-center justify-center px-4">
      <div className="relative w-28 h-28 flex items-center justify-center">
        <motion.div className="absolute inset-0 rounded-full border-2 border-emerald-500/20" animate={{ scale: [1, 1.15, 1], opacity: [0.5, 0.2, 0.5] }} transition={{ duration: 1.6, repeat: Infinity }} />
        <motion.div className="absolute inset-0 rounded-full border-t-2 border-emerald-400" animate={{ rotate: 360 }} transition={{ duration: 1.2, repeat: Infinity, ease: 'linear' }} />
        <Radar className="text-emerald-400" size={36} />
      </div>
      <h2 className="text-slate-200 font-semibold mt-8">正在透视交易风险</h2>
      <AnimatePresence mode="wait"><motion.p key={step} initial={{ opacity: 0, y: 6 }} animate={{ opacity: 1, y: 0 }} exit={{ opacity: 0, y: -6 }} className="text-sm text-slate-500 mt-2 h-5">{LOADING_STEPS[step]}</motion.p></AnimatePresence>
    </div>
  );
};

const ErrorView = ({ message, onRetry, onReset }) => (
  <div className="min-h-screen flex flex-col items-center justify-center px-4 text-center">
    <div className="inline-flex items-center justify-center w-16 h-16 rounded-2xl bg-red-500/10 border border-red-500/30 mb-5"><XCircle className="text-red-400" size={32} /></div>
    <h2 className="text-xl font-semibold text-slate-100">检测失败</h2>
    <p className="text-sm text-slate-400 mt-2 max-w-sm">{message}</p>
    <div className="flex gap-3 mt-6">
      <button onClick={onRetry} className="px-5 py-2.5 rounded-xl bg-emerald-500 hover:bg-emerald-400 text-slate-950 font-medium">重试</button>
      <button onClick={onReset} className="px-5 py-2.5 rounded-xl bg-slate-800 hover:bg-slate-700 text-slate-200">返回</button>
    </div>
  </div>
);

const ReportView = ({ report, target, onReset }) => {
  const dims = useMemo(() => buildDimensions(report), [report]);
  const ov = report?.overview || {};
  const riskScore = ov.risk_score || 0;
  const level = getLevel(riskScore);
  const [openKey, setOpenKey] = useState(() => dims.find(d => d.level === 'high')?.key || dims[0]?.key);

  const isEmpty = !report || Object.keys(report).length === 0;
  const isZero = !isEmpty && ov.total_trades === 0;
  const allSafe = dims.every(d => d.level === 'low');

  return (
    <div className="max-w-3xl mx-auto px-4 py-6 md:py-10">
      <button onClick={onReset} className="flex items-center gap-1.5 text-sm text-slate-400 hover:text-slate-200 mb-5"><ArrowLeft size={16} /> 重新检测</button>
      {target && <div className="text-xs text-slate-500 mb-4 truncate">分析目标：{target}</div>}

      {isEmpty || isZero ? (
        <div className="rounded-2xl border border-slate-800 bg-slate-900/50 p-10 text-center">
          {isEmpty ? <XCircle className="mx-auto text-slate-500 mb-4" size={48} /> : <Search className="mx-auto text-emerald-400 mb-4" size={48} />}
          <h3 className="text-lg font-semibold text-slate-200">{isEmpty ? '未获取到数据' : '暂无交易记录'}</h3>
        </div>
      ) : (
        <>
          <div className="rounded-3xl border bg-slate-900/50 p-6 md:p-8 mb-6 text-center" style={{ borderColor: LEVEL_STYLES[level].hex + '40' }}>
            <RiskGauge score={riskScore} level={level} />
            <div className="mt-6 inline-flex flex-wrap items-center justify-center gap-4 text-sm bg-slate-950/50 border border-slate-800/60 rounded-xl px-5 py-3">
              <div className="flex gap-2"><span className="text-slate-500">交易笔数</span><span className="text-slate-200">{fmt.num(ov.total_trades, 0)} 笔</span></div>
              <div className="w-px h-4 bg-slate-800 hidden sm:block" />
              <div className="flex gap-2"><span className="text-slate-500">时间范围</span><span className="text-slate-200">{fmt.date(ov.start_time)} 至 {fmt.date(ov.end_time)}</span></div>
            </div>
          </div>

          {dims.length === 0 ? (
             <div className="rounded-2xl border border-slate-800 bg-slate-900/50 p-10 text-center"><ShieldCheck className="mx-auto text-emerald-400 mb-4" size={48} /><h3>未检测到可分析风险数据</h3></div>
          ) : (
            <div className="space-y-3">
              {allSafe && (
                <div className="rounded-2xl bg-emerald-500/5 border border-emerald-500/20 p-5 flex gap-3">
                  <ShieldCheck className="text-emerald-400 shrink-0" size={22} />
                  <div><h3 className="text-emerald-300 font-semibold">未检测到高危记录</h3><p className="text-sm text-slate-400 mt-1">未发现明显的高危行为，以下为详细数据供参考。</p></div>
                </div>
              )}
              {dims.map(d => <DimensionCard key={d.key} dim={d} expanded={openKey === d.key} onToggle={() => setOpenKey(openKey === d.key ? null : d.key)} />)}
            </div>
          )}
        </>
      )}
      <p className="text-xs text-slate-600 text-center mt-8 max-w-lg mx-auto">本报告仅供参考，不构成投资建议。加密资产交易风险极高，请独立决策。</p>
    </div>
  );
};

/* =========================================================================
 * 9. 根组件
 * ========================================================================= */
export default function App() {
  const { status, report, error, target, run, reset, retry } = useRiskAnalysis();

  return (
    <div className="min-h-screen bg-gradient-to-b from-slate-950 to-slate-900 text-slate-100 antialiased selection:bg-emerald-500/30">
      {status === 'idle' && <InputView onSubmit={run} />}
      {status === 'loading' && <LoadingView />}
      {status === 'error' && <ErrorView message={error} onRetry={retry} onReset={reset} />}
      {status === 'success' && <ReportView report={report} target={target} onReset={reset} />}
    </div>
  );
}