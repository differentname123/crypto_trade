import React, { useState } from 'react';

// ==========================================
// 1. 多语言配置及切换逻辑
// ==========================================
const i18nDict = {
  zh: {
    title: "网格策略参数与资金计算器",
    btn_volatility: "🚀 信号雷达",
    btn_best_params: "💡 最优参数查询",
    select_op: "🏆 请选择你需要计算的类型：",
    ops: [
      { title: "1. 计算所需总资金", desc: "输入所有参数，计算运行该策略一共需要占用多少资金。" },
      { title: "2. 计算最大承受波动", desc: "根据已有资金和网格设置，计算你最多能承受多大的价格波动。" },
      { title: "3. 计算最小网格间距", desc: "根据已有资金和预期风险，计算网格间距最小可以设置到多少。" },
      { title: "4. 计算每格最大交易量", desc: "固定资金、间距和最大波动后，计算每个网格最多能买卖多少数量。" }
    ],
    sec_basic: "基础设置",
    lbl_dir: "开仓方向",
    tt_dir: "做多：预期价格上涨。\n做空：预期价格下跌。",
    dir_long: "做多 (Long)",
    dir_short: "做空 (Short)",
    lbl_lev: "杠杆倍数",
    tt_lev: "策略运行的真实杠杆倍数。\n杠杆数值越大，单次所需占用资金越少，但爆仓强平的风险也越高。",
    lbl_ip: "初始价格",
    tt_ip: "策略开始运行时的市场标杆基准价格。",
    sec_core: "核心策略参数",
    lbl_loss: "最大承受波动 (%)",
    tt_loss: "你希望策略能承受的最大反向价格波动百分比。\n例如：做多时，期望价格最多下跌多少而不爆仓。",
    lbl_step: "网格间距 (%)",
    tt_step: "相邻两个网格（买卖点）之间的价格百分比差值。\n数值越小，网格越密集，交易越频繁，所需资金也会增多。",
    lbl_qty: "每格交易数量",
    tt_qty: "每个网格触发时买入或卖出的资产数量。\n该数值直接影响总资金的占用大小。",
    lbl_margin: "计划投入资金 (U)",
    tt_margin: "你打算投入该网格策略的总 USDT 金额。\n此项将作为反向推算的资金限制条件。",
    btn_calc: "开始计算",
    calcing: "计算中...",
    success_calc: "✅ 计算成功",
    success_sim: "✅ 计算完成",
    err_calc: "❌ 计算出错",
    res_req_margin: "所需资金总计 (U)",
    res_margin_desc: "根据上述设置，策略在极限情况下需要占用的资金为: ",
    res_max_loss: "最大承受波动",
    res_limit_price: "对应极限价格",
    res_min_step: "最小网格间距",
    res_max_qty: "每格最大交易量",
    res_util: "资金利用率",
    res_actual: "实际消耗资金",
    res_est_grids: "预计网格数量",
    err_params: "所有数值参数必须 > 0",
    err_dir: "direction 参数必须是 'long' 或 'short'",
    err_long_100: "做多时，价格最大跌幅不能 >= 100.0% (价格不能跌破 0)。",
    err_loop: "计算超出限制，请检查网格间距是否设置得过小。",
    err_avail: "投入资金必须 > 0",
    err_not_found: "在范围内无法找到满足条件的解。可能是投入资金严重不足或参数设置不合理。"
  },
  en: {
    title: "Grid Strategy Parameter & Margin Calculator",
    btn_volatility: "🚀 Volatility Radar",
    btn_best_params: "💡 Best Params Query",
    select_op: "🏆 Please select a calculation type:",
    ops: [
      { title: "1. Calc Total Margin", desc: "Input all parameters to calculate the total margin required to run the strategy." },
      { title: "2. Calc Max Tolerance", desc: "Calculate the maximum price volatility you can endure based on available funds." },
      { title: "3. Calc Min Grid Spacing", desc: "Calculate the tightest grid spacing possible with your current funds and risk." },
      { title: "4. Calc Max Qty per Grid", desc: "Calculate the maximum trade size per grid given fixed funds, spacing, and risk." }
    ],
    sec_basic: "Basic Settings",
    lbl_dir: "Direction",
    tt_dir: "Long: Expecting prices to rise.\nShort: Expecting prices to fall.",
    dir_long: "Long",
    dir_short: "Short",
    lbl_lev: "Leverage",
    tt_lev: "The actual leverage multiplier.\nHigher leverage requires less initial margin but significantly increases liquidation risk.",
    lbl_ip: "Initial Price",
    tt_ip: "The benchmark market price when the strategy starts.",
    sec_core: "Core Strategy Parameters",
    lbl_loss: "Max Expected Volatility (%)",
    tt_loss: "The maximum adverse price movement percentage you want the strategy to withstand without liquidation.",
    lbl_step: "Grid Spacing (%)",
    tt_step: "The percentage price gap between two adjacent grids.\nSmaller values mean denser grids, more frequent trading, and higher margin requirements.",
    lbl_qty: "Qty per Grid",
    tt_qty: "The asset amount bought or sold at each grid trigger.\nDirectly affects total margin consumed.",
    lbl_margin: "Planned Funds (U)",
    tt_margin: "Total USDT you plan to allocate.\nActs as the absolute constraint for calculations.",
    btn_calc: "Start Calculation",
    calcing: "Calculating...",
    success_calc: "✅ Calculation Successful",
    success_sim: "✅ Calculation Complete",
    err_calc: "❌ Calculation Error",
    res_req_margin: "Total Required Margin (U)",
    res_margin_desc: "Based on these settings, the absolute maximum margin required is: ",
    res_max_loss: "Max Tolerable Volatility",
    res_limit_price: "Corresponding Limit Price",
    res_min_step: "Min Grid Spacing",
    res_max_qty: "Max Qty per Grid",
    res_util: "Fund Utilization",
    res_actual: "Actual Funds Consumed",
    res_est_grids: "Estimated Grids",
    err_params: "All numeric parameters must be > 0",
    err_dir: "Direction parameter must be 'long' or 'short'",
    err_long_100: "When going Long, max drop cannot be >= 100.0% (price cannot drop below 0).",
    err_loop: "Calculation exceeded limits. Please check if grid spacing is set too small.",
    err_avail: "Planned funds must be > 0",
    err_not_found: "Could not find a valid solution. Funds may be too low or parameters are unreasonable."
  }
};

// ==========================================
// 2. 核心算法逻辑
// ==========================================
function calculate_multi_group_margin(params, t) {
  let { leverage, target_loss_percent, max_grids_per_group, fixed_qty, add_step_percent, initial_price, direction } = params;

  if (!(leverage > 0) || !(target_loss_percent > 0) || !(add_step_percent > 0) || !(fixed_qty > 0) || !(max_grids_per_group > 0)) {
    throw new Error(t.err_params);
  }
  if (direction !== 'long' && direction !== 'short') {
    throw new Error(t.err_dir);
  }
  if (direction === 'long' && target_loss_percent >= 100.0) {
    throw new Error(t.err_long_100);
  }

  let r = add_step_percent / 100.0;
  let sign = direction === 'long' ? 1 : -1;
  let target_price = direction === 'long'
    ? initial_price * (1.0 - target_loss_percent / 100.0)
    : initial_price * (1.0 + target_loss_percent / 100.0);

  let current_price = initial_price;
  let total_margin_all_groups = 0.0;
  let total_grids = 0;
  let infiniteLoopGuard = 0;

  const is_active = (cp, tp) => direction === 'long' ? cp >= tp : cp <= tp;

  while (is_active(current_price, target_price)) {
    let group_qty = 0.0;
    let group_cost = 0.0;
    let group_max_margin = 0.0;
    let grids_in_group = 0;

    while (grids_in_group < max_grids_per_group && is_active(current_price, target_price)) {
      if (infiniteLoopGuard++ > 100000) {
        throw new Error(t.err_loop);
      }

      group_qty += fixed_qty;
      group_cost += fixed_qty * current_price;
      grids_in_group += 1;
      total_grids += 1;

      let upnl_at_open = sign * (group_qty * current_price - group_cost);
      let required_for_margin = (group_cost / leverage) - upnl_at_open;
      if (required_for_margin > group_max_margin) group_max_margin = required_for_margin;

      let next_price = direction === 'long' ? current_price * (1 - r) : current_price * (1 + r);
      let check_price = direction === 'long' ? Math.max(next_price, target_price) : Math.min(next_price, target_price);

      let upnl_at_bottom = sign * (group_qty * check_price - group_cost);
      let required_for_survival = -upnl_at_bottom;
      if (required_for_survival > group_max_margin) group_max_margin = required_for_survival;

      current_price = next_price;
    }

    let upnl_at_global_target = sign * (group_qty * target_price - group_cost);
    let required_at_global_target = (group_cost / leverage) - upnl_at_global_target;
    if (required_at_global_target > group_max_margin) group_max_margin = required_at_global_target;

    total_margin_all_groups += group_max_margin;
  }

  return { total_margin: total_margin_all_groups, total_grids: total_grids };
}

function universal_margin_solver(target_param_name, available_margin, base_params, low_bound, high_bound, positive_correlation, t, tolerance = 0.0001) {
  if (!(available_margin > 0)) return { status: "failed", message: t.err_avail };

  let best_val = null;
  let best_result = null;
  let low = low_bound, high = high_bound;
  let guard = 0;

  while ((high - low) > tolerance && guard++ < 200) {
    let mid = (low + high) / 2.0;
    let test_params = { ...base_params };
    test_params[target_param_name] = mid;

    try {
      let current_result = calculate_multi_group_margin(test_params, t);
      let current_margin = current_result.total_margin;

      if (current_margin > available_margin) {
        if (positive_correlation) high = mid; else low = mid;
      } else {
        best_val = mid;
        best_result = current_result;
        if (positive_correlation) low = mid; else high = mid;
      }
    } catch (e) {
      if (positive_correlation) high = mid; else low = mid;
    }
  }

  if (best_val === null) {
    return { status: "failed", message: t.err_not_found };
  }

  return {
    status: "success",
    optimal_value: Number(best_val.toFixed(5)),
    actual_margin_used: Number(best_result.total_margin.toFixed(2)),
    utilization_rate: ((best_result.total_margin / available_margin) * 100).toFixed(2) + "%",
    total_grids: best_result.total_grids
  };
}

// ==========================================
// 3. React 视图组件
// ==========================================
export default function App() {
  const [lang, setLang] = useState('zh');
  const [calcMode, setCalcMode] = useState("0");
  const [form, setForm] = useState({
    direction: 'long',
    leverage: 100,
    initialPrice: 2000,
    targetLossPercent: 30,
    addStepPercent: 1,
    fixedQty: 1,
    availableMargin: 1000
  });
  const [result, setResult] = useState(null);

  const t = i18nDict[lang];

  const handleInputChange = (field, value) => {
    setForm(prev => ({ ...prev, [field]: value }));
  };

  const handleCalculate = () => {
    const params = {
      direction: form.direction,
      leverage: parseFloat(form.leverage),
      initial_price: parseFloat(form.initialPrice),
      max_grids_per_group: 100000,
      target_loss_percent: parseFloat(form.targetLossPercent),
      add_step_percent: parseFloat(form.addStepPercent),
      fixed_qty: parseFloat(form.fixedQty)
    };
    const availableMargin = parseFloat(form.availableMargin);

    try {
      if (calcMode === "0") {
        const res = calculate_multi_group_margin(params, t);
        setResult({
          status: 'success',
          type: 'margin',
          data: res,
          title: t.res_req_margin,
          value: `${res.total_margin.toFixed(2)} U`,
          desc: `${t.res_margin_desc}${res.total_margin.toFixed(2)} U。`
        });
      } else {
        let res;
        if (calcMode === "1") {
          const maxLossBound = params.direction === 'long' ? 99.9 : 1000.0;
          res = universal_margin_solver("target_loss_percent", availableMargin, params, 0.1, maxLossBound, true, t, 0.001);
          if (res.status === 'success') {
            const limitPrice = params.direction === 'long'
              ? params.initial_price * (1.0 - res.optimal_value / 100.0)
              : params.initial_price * (1.0 + res.optimal_value / 100.0);
            res.valueDisplay = `${res.optimal_value}%`;
            res.subDisplay = `≈ ${t.res_limit_price}: ${limitPrice.toFixed(4)}`;
            res.title = t.res_max_loss;
          }
        } else if (calcMode === "2") {
          res = universal_margin_solver("add_step_percent", availableMargin, params, 0.01, 100.0, false, t, 0.001);
          if (res.status === 'success') {
            res.valueDisplay = `${res.optimal_value}%`;
            res.title = t.res_min_step;
          }
        } else if (calcMode === "3") {
          res = universal_margin_solver("fixed_qty", availableMargin, params, 0.0001, 1000000.0, true, t, 0.0001);
          if (res.status === 'success') {
            res.valueDisplay = `${res.optimal_value}`;
            res.title = t.res_max_qty;
          }
        }

        if (res.status === 'success') {
          setResult({ status: 'success', type: 'solver', data: res });
        } else {
          throw new Error(res.message);
        }
      }
    } catch (error) {
      setResult({ status: 'error', message: error.message });
    }
  };

  // 高复用原生 Tailwind Tooltip 组件
  const LabelTooltip = ({ label, tooltip }) => (
    <div className="flex items-center mb-2 text-sm font-medium text-gray-600 dark:text-gray-300">
      <span>{label}</span>
      <div className="relative group inline-flex items-center ml-1 cursor-help">
        <span className="text-gray-400 group-hover:text-blue-500 transition-colors px-1">ⓘ</span>
        <div className="absolute bottom-full left-1/2 -translate-x-1/2 mb-2 hidden group-hover:block w-56 p-2.5 bg-gray-900 text-white text-xs rounded-lg shadow-xl z-50 whitespace-pre-line pointer-events-none">
          {tooltip}
          <div className="absolute top-full left-1/2 -translate-x-1/2 border-4 border-transparent border-t-gray-900"></div>
        </div>
      </div>
    </div>
  );

  return (
    <div className="min-h-screen bg-gray-50 dark:bg-gray-900 p-4 md:p-8 flex justify-center text-gray-800 dark:text-gray-200 font-sans">
      <div className="w-full max-w-4xl bg-white dark:bg-gray-800 p-6 md:p-10 rounded-2xl shadow-xl relative">

        {/* 语言切换 */}
        <div className="absolute top-4 right-4 md:top-6 md:right-6">
          <select
            className="p-2 border border-gray-200 dark:border-gray-700 rounded-lg bg-gray-50 dark:bg-gray-900 text-sm outline-none focus:border-blue-500 transition-colors cursor-pointer"
            value={lang}
            onChange={(e) => setLang(e.target.value)}
          >
            <option value="zh">🇨🇳 简体中文</option>
            <option value="en">🇺🇸 English</option>
          </select>
        </div>

        <h1 className="text-2xl md:text-3xl font-bold text-center mt-12 md:mt-4 mb-8 text-gray-900 dark:text-white">
          {t.title}
        </h1>

        <div className="flex flex-wrap justify-center gap-4 mb-8">
          <a href="https://quant-db-ft.zhuxiaohu98.workers.dev/" target="_blank" rel="noreferrer" className="inline-flex items-center px-6 py-2.5 bg-orange-50 text-orange-700 border border-orange-200 rounded-full font-semibold hover:bg-orange-100 hover:-translate-y-0.5 transition-all shadow-sm">
            {t.btn_volatility}
          </a>
          <a href="https://crypto-grid-eval.pages.dev/" target="_blank" rel="noreferrer" className="inline-flex items-center px-6 py-2.5 bg-orange-50 text-orange-700 border border-orange-200 rounded-full font-semibold hover:bg-orange-100 hover:-translate-y-0.5 transition-all shadow-sm">
            {t.btn_best_params}
          </a>
        </div>

        {/* 功能选择 */}
        <div className="mb-10">
          <label className="block mb-4 font-semibold text-lg text-gray-700 dark:text-gray-300">{t.select_op}</label>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            {t.ops.map((op, idx) => {
              const isActive = calcMode === String(idx);
              return (
                <label key={idx} className={`relative p-5 border-2 rounded-xl cursor-pointer transition-all duration-200 ${isActive ? 'border-blue-600 bg-blue-50 dark:bg-blue-900/20' : 'border-gray-200 dark:border-gray-700 hover:border-blue-400'}`}>
                  <input type="radio" name="calcMode" value={String(idx)} checked={isActive} onChange={(e) => setCalcMode(e.target.value)} className="absolute top-5 right-5 w-4 h-4 text-blue-600" />
                  <h3 className="font-bold text-lg mb-1 pr-6 text-gray-900 dark:text-gray-100">{op.title}</h3>
                  <p className="text-sm text-gray-500 dark:text-gray-400">{op.desc}</p>
                </label>
              );
            })}
          </div>
        </div>

        {/* 表单区域 */}
        <div className="mb-8 space-y-8">
          <div>
            <h3 className="text-lg font-bold border-b border-gray-200 dark:border-gray-700 pb-2 mb-5 text-gray-800 dark:text-gray-200">{t.sec_basic}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              <div>
                <LabelTooltip label={t.lbl_dir} tooltip={t.tt_dir} />
                <select className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.direction} onChange={(e) => handleInputChange('direction', e.target.value)}>
                  <option value="long">{t.dir_long}</option>
                  <option value="short">{t.dir_short}</option>
                </select>
              </div>
              <div>
                <LabelTooltip label={t.lbl_lev} tooltip={t.tt_lev} />
                <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.leverage} onChange={(e) => handleInputChange('leverage', e.target.value)} />
              </div>
              <div>
                <LabelTooltip label={t.lbl_ip} tooltip={t.tt_ip} />
                <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.initialPrice} onChange={(e) => handleInputChange('initialPrice', e.target.value)} />
              </div>
            </div>
          </div>

          <div>
            <h3 className="text-lg font-bold border-b border-gray-200 dark:border-gray-700 pb-2 mb-5 text-gray-800 dark:text-gray-200">{t.sec_core}</h3>
            <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
              {calcMode !== "1" && (
                <div className="animate-in fade-in duration-300">
                  <LabelTooltip label={t.lbl_loss} tooltip={t.tt_loss} />
                  <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.targetLossPercent} onChange={(e) => handleInputChange('targetLossPercent', e.target.value)} />
                </div>
              )}
              {calcMode !== "2" && (
                <div className="animate-in fade-in duration-300">
                  <LabelTooltip label={t.lbl_step} tooltip={t.tt_step} />
                  <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.addStepPercent} onChange={(e) => handleInputChange('addStepPercent', e.target.value)} />
                </div>
              )}
              {calcMode !== "3" && (
                <div className="animate-in fade-in duration-300">
                  <LabelTooltip label={t.lbl_qty} tooltip={t.tt_qty} />
                  <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.fixedQty} onChange={(e) => handleInputChange('fixedQty', e.target.value)} />
                </div>
              )}
              {calcMode !== "0" && (
                <div className="animate-in fade-in duration-300">
                  <LabelTooltip label={t.lbl_margin} tooltip={t.tt_margin} />
                  <input type="number" className="w-full p-3 border border-gray-300 dark:border-gray-600 rounded-lg bg-white dark:bg-gray-800 focus:outline-none focus:ring-2 focus:ring-blue-500 transition-all" value={form.availableMargin} onChange={(e) => handleInputChange('availableMargin', e.target.value)} />
                </div>
              )}
            </div>
          </div>
        </div>

        <button onClick={handleCalculate} className="w-full p-4 bg-blue-600 text-white rounded-xl font-bold text-lg hover:bg-blue-700 hover:-translate-y-0.5 active:translate-y-0 transition-all shadow-md">
          {t.btn_calc}
        </button>

        {/* 结果展示区 */}
        {result && (
          <div className="mt-8 p-6 bg-gray-50 dark:bg-gray-800/50 border border-gray-200 dark:border-gray-700 rounded-xl animate-in fade-in zoom-in-95 duration-300">
            {result.status === 'success' ? (
              <>
                <h4 className="text-green-700 dark:text-green-500 font-bold mb-4">{t.success_sim}</h4>
                {result.type === 'margin' ? (
                  <>
                    <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{result.title}</h3>
                    <p className="text-4xl font-black text-blue-600 mt-2 mb-4">{result.value}</p>
                    <p className="text-sm text-gray-500 dark:text-gray-400 mb-6">{result.desc}</p>
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
                      <div><span className="text-sm text-gray-500">{t.res_est_grids}</span><div className="mt-1 font-semibold p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">{result.data.total_grids}</div></div>
                    </div>
                  </>
                ) : (
                  <>
                    <h3 className="text-lg font-bold text-gray-900 dark:text-gray-100">{result.data.title}</h3>
                    <p className="text-4xl font-black text-blue-600 mt-2 mb-1">{result.data.valueDisplay}</p>
                    {result.data.subDisplay && <p className="text-base text-gray-500 dark:text-gray-400 mb-6">{result.data.subDisplay}</p>}

                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mt-6">
                      <div><span className="text-sm text-gray-500">{t.res_util}</span><div className="mt-1 font-semibold p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">{result.data.utilization_rate}</div></div>
                      <div><span className="text-sm text-gray-500">{t.res_actual}</span><div className="mt-1 font-semibold p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">{result.data.actual_margin_used} U</div></div>
                      <div><span className="text-sm text-gray-500">{t.res_est_grids}</span><div className="mt-1 font-semibold p-3 bg-white dark:bg-gray-800 border border-gray-200 dark:border-gray-700 rounded-lg">{result.data.total_grids}</div></div>
                    </div>
                  </>
                )}
              </>
            ) : (
              <h4 className="text-red-600 dark:text-red-400 font-bold">{result.message}</h4>
            )}
          </div>
        )}

      </div>
    </div>
  );
}