# -*- coding: utf-8 -*-
import pandas as pd
import os

from app.signal_trade_lite.martin_strategy_1m import factor_044_3
# 1. 复用底层回测引擎核心函数 (从你提供的第三段代码中导入)
from app.signal_trade_lite.martin_strategy_backest import run_stage1, TimelineReplayer, evaluate_free_ride
from app.signal_trade_lite.strategy_evaluator_comb import _normalize_trades_df


# 2. 复用因子生成函数 (从你提供的第二段代码中导入，假设文件名为 factors.py)
# 如果你的因子函数在其他文件，请自行修改此路径

# 3. 复用数据归一化函数 (从你提供的第一段代码中导入，假设文件名为 portfolio_backtest.py)
# 注意：_normalize_trades_df 依赖 evaluate_free_ride，已经在其内部调用

def run_minimal_backtest():
    # ==========================================
    # 1. 核心参数配置
    # ==========================================
    csv_path = r"W:\project\python_project\oke_auto_trade\kline_data\UNIUSDT_1m_2021-01-01_merged.csv"

    symbol = "UNIUSDT"
    strategy_name = "factor_044_3"
    direction = "Long"

    margin = 5
    add_step = 0.010  # step_pct 1%
    tp_step = 0.005  # tp_pct 0.5%
    qty_mult = 2.0  # qty_mult 2

    # ==========================================
    # 2. 加载 K线数据
    # ==========================================
    print(f"📂 正在加载数据: {csv_path}")
    df = pd.read_csv(csv_path)

    # ==========================================
    # 3. 生成信号
    # ==========================================
    print(f"🔍 正在计算信号: {strategy_name}")
    df = factor_044_3(df)

    # 044是向下假突破（做多信号），需要将结果映射到 run_stage1 识别的 long_signal 列
    df['long_signal'] = df['signal']
    if 'open_time' not in df.columns and 'timestamp' in df.columns:
        df['open_time'] = df['timestamp']
    # ==========================================
    # 4. 运行 Stage 1: 生成马丁平行宇宙缓存
    # ==========================================
    print(f"⚙️ 正在运行 Stage 1 (计算引擎)...")
    cycles_df = run_stage1(
        df,
        fee_rate=0.0005,  # 默认手续费
        add_step=add_step,
        tp_step=tp_step,
        multiplier=qty_mult,
        max_layer_hard=512,
        verbose=False  # 关闭冗长打印
    )

    # ==========================================
    # 5. 运行 Stage 2: 指定 Margin 生成逐笔明细
    # ==========================================
    print(f"🔄 正在运行 Stage 2 (时间线重放, Margin={margin})...")
    replayer = TimelineReplayer(cycles_df)
    trades_df = replayer.run(margin=margin)

    # ==========================================
    # 6. 运行 Stage 3: 收益归一化 (与榜单严格对齐)
    # ==========================================
    print(f"📊 正在进行收益归一化...")
    norm_df, summ = _normalize_trades_df(trades_df, cycles_df, margin)

    # 补充元数据列，确保与 extract_target_trades_csv 产出的宽表格式 100% 一致
    norm_df["symbol"] = symbol
    norm_df["strategy"] = strategy_name
    norm_df["direction"] = direction
    norm_df["margin"] = margin
    norm_df["add_step"] = add_step
    norm_df["tp_step"] = tp_step
    norm_df["multiplier"] = qty_mult

    # ==========================================
    # 7. 落盘保存 CSV
    # ==========================================
    out_dir = "./"
    os.makedirs(out_dir, exist_ok=True)

    # 构造一致的文件名
    mtag = f"_x{qty_mult:g}"
    out_filename = f"trades_{symbol}_{strategy_name}_{direction}_M{margin}_add{add_step:.3f}_tp{tp_step:.3f}{mtag}.csv"
    out_filepath = os.path.join(out_dir, out_filename)

    norm_df.to_csv(out_filepath, index=False, encoding="utf-8-sig")

    # ==========================================
    # 8. 打印结果
    # ==========================================
    print("\n" + "=" * 60)
    print(f"🎉 回测文件生成成功: {out_filepath}")
    print(f"📈 [交易概览] 实际开仓: {summ['实际开仓数']} 笔 | 胜率: {summ['胜率(%)']}%")
    print(f"💰 [收益概览] 净利润: {summ['净利润(M倍)']} M倍 | 爆仓次数: {summ['爆仓次数']}")
    print("=" * 60)


if __name__ == "__main__":
    run_minimal_backtest()