# -- coding: utf-8 --
""":authors:
    zhuxiaohu, AI Assistant
:create_date:
    2026/9/18
:description:
    基于Beta调整与固定期限的横截面统计套利策略 (修正版)
    低耦合架构：先逐个币种回测并保存交易明细 -> 最后汇总分析
"""
import pandas as pd
import numpy as np
import os
import glob
from tqdm import tqdm


# ==========================================
# 1. 全局绝对性配置参数 (方便后续修改与网格搜索)
# ==========================================
# ==========================================
# 1. 全局绝对性配置参数 (方便后续修改与网格搜索)
# ==========================================
class Config:
    # 基础路径配置
    DATA_DIR = r"W:\project\python_project\oke_auto_trade\kline_data"
    BASE_OUTPUT_DIR = r"trade_results"  # 改为基础输出目录
    BTC_SYMBOL = "BTCUSDT"

    # 策略参数
    BETA_WINDOW_DAYS = 30  # 计算Beta的时间窗口(天)
    SIGNAL_WINDOW_HOURS = 24  # 计算残差收益的窗口(小时)
    Z_SCORE_THRESHOLD = 2.0  # Z值极值阈值
    HOLDING_PERIOD_HOURS = 6  # 固定持仓时间(小时)

    # 成本参数
    FEE_RATE = 0.001  # 单边综合成本(滑点+手续费+资金费率)，0.1%

    # 派生参数
    BETA_WINDOW_HOURS = BETA_WINDOW_DAYS * 24

    # 【核心修改】：动态生成带有参数标识的专属输出子文件夹
    # 例如： trade_results/Z2.0_H6_B30_S24
    PARAM_FOLDER = f"Z{Z_SCORE_THRESHOLD}_H{HOLDING_PERIOD_HOURS}_B{BETA_WINDOW_DAYS}_S{SIGNAL_WINDOW_HOURS}"
    OUTPUT_DIR = os.path.join(BASE_OUTPUT_DIR, PARAM_FOLDER)


# 确保当前参数组合的专属输出目录存在
if not os.path.exists(Config.OUTPUT_DIR):
    os.makedirs(Config.OUTPUT_DIR)


# ==========================================
# 2. 核心信号计算与指标加工模块
# ==========================================
def calculate_indicators(df_alt, df_btc):
    """
    计算Beta、残差收益、Z-score等核心指标
    要求 df_alt 和 df_btc 的 index 必须是对齐的 datetime 格式
    """
    df = pd.DataFrame(index=df_alt.index)
    df['close'] = df_alt['close']
    df['volume'] = df_alt['volume']  # 用于后续计算成交额分组
    df['btc_close'] = df_btc['close']

    # 1. 计算1小时对数收益率
    df['ret_1h'] = np.log(df['close'] / df['close'].shift(1))
    df['btc_ret_1h'] = np.log(df['btc_close'] / df['btc_close'].shift(1))

    # 计算24小时对数收益率 (通过1h收益率滚动求和，或直接使用 shift(24))
    df['ret_24h'] = np.log(df['close'] / df['close'].shift(Config.SIGNAL_WINDOW_HOURS))
    df['btc_ret_24h'] = np.log(df['btc_close'] / df['btc_close'].shift(Config.SIGNAL_WINDOW_HOURS))

    # 2. 计算30天滚动的 Beta
    # Beta = Cov(Alt_1h, BTC_1h) / Var(BTC_1h)
    cov = df['ret_1h'].rolling(window=Config.BETA_WINDOW_HOURS).cov(df['btc_ret_1h'])
    var = df['btc_ret_1h'].rolling(window=Config.BETA_WINDOW_HOURS).var()
    df['beta'] = cov / var

    # 【核心修正】：用最近24小时信号窗口*之前*的30天Beta
    df['beta_shifted'] = df['beta'].shift(Config.SIGNAL_WINDOW_HOURS)

    # 3. 计算24小时残差收益
    # 真实异常收益 = 自身24h收益 - (事先估计的Beta * BTC的24h收益)
    df['residual_24h'] = df['ret_24h'] - (df['beta_shifted'] * df['btc_ret_24h'])

    # 4. 自身历史标准化 (计算Z值)
    # 【核心修正】：计算均值和标准差时，必须排除最近的24小时，且使用过去30天的数据
    df['hist_res_mean'] = df['residual_24h'].shift(Config.SIGNAL_WINDOW_HOURS).rolling(
        window=Config.BETA_WINDOW_HOURS).mean()
    df['hist_res_std'] = df['residual_24h'].shift(Config.SIGNAL_WINDOW_HOURS).rolling(
        window=Config.BETA_WINDOW_HOURS).std()

    # 计算Z值
    df['z_score'] = (df['residual_24h'] - df['hist_res_mean']) / df['hist_res_std']

    # 5. 附加信息：计算30天日均成交额 (用于后续分组验证)
    # 假设 volume 是币的数量，成交额大致 = volume * close
    df['turnover'] = df['volume'] * df['close']
    df['avg_turnover_30d'] = df['turnover'].rolling(window=Config.BETA_WINDOW_HOURS).mean() * 24

    return df


# ==========================================
# 3. 预计算动态截面中位数 & 单标的回测模块
# ==========================================
def generate_market_median(symbols):
    """预计算全市场的动态截面中位数水位线，带有本地缓存复用功能"""

    # 将缓存文件存放在最外层的 BASE_OUTPUT_DIR 中，作为公共资源被各个参数组合复用
    # 文件名绑定 BETA_WINDOW_DAYS，如果以后修改了 Beta 窗口天数，它会自动重新计算一份新的
    cache_path = os.path.join(Config.BASE_OUTPUT_DIR, f"market_median_B{Config.BETA_WINDOW_DAYS}.csv")

    # ============ 1. 尝试命中缓存 ============
    if os.path.exists(cache_path):
        print(f"检测到已存在的全市场截面中位数缓存: {cache_path}")
        print("直接加载缓存，跳过重复计算...\n")
        df_cache = pd.read_csv(cache_path)
        df_cache['open_time'] = pd.to_datetime(df_cache['open_time'])
        df_cache.set_index('open_time', inplace=True)
        # 返回 pandas Series 格式保持前后兼容
        return df_cache['median_turnover']

    # ============ 2. 缓存未命中，执行全量计算 ============
    print("未检测到缓存，正在预计算全市场动态截面中位数水位线 (计算量较大请耐心等待)...")
    turnover_dfs = []

    for symbol in tqdm(symbols):
        if symbol == Config.BTC_SYMBOL:
            continue

        kline_path = os.path.join(Config.DATA_DIR, f"{symbol}_1h_2021-01-01_merged.csv")
        if not os.path.exists(kline_path):
            continue

        # 读取 open_time
        df = pd.read_csv(kline_path, usecols=['open_time', 'close', 'volume'])
        # 指定 unit='ms' 将毫秒级时间戳转为 datetime 格式
        df['open_time'] = pd.to_datetime(df['open_time'], unit='ms')
        df.set_index('open_time', inplace=True)

        # 计算该币种的30天日均成交额
        turnover = df['volume'] * df['close']
        avg_turnover_30d = turnover.rolling(window=Config.BETA_WINDOW_HOURS).mean() * 24
        turnover_dfs.append(avg_turnover_30d.rename(symbol))

    if not turnover_dfs:
        return pd.Series(dtype=float)

    # 拼接全市场成交额大表，并求横截面中位数
    all_turnovers = pd.concat(turnover_dfs, axis=1)
    market_median_series = all_turnovers.median(axis=1)
    market_median_series.name = 'median_turnover'  # 为存入 CSV 设置列名

    # ============ 3. 落盘保存缓存 ============
    print(f"计算完成！正在将全市场中位线保存至缓存: {cache_path}")
    market_median_series.to_csv(cache_path, header=True, index_label='open_time')

    print("动态中位数基准线准备就绪！\n")
    return market_median_series


def backtest_single_symbol(symbol, df, market_median_series):
    """
    执行独立配对交易逻辑，生成交易记录
    """
    trades = []

    in_position = False
    cooldown = False  # 冷却状态：平仓后等待回落

    entry_time = None
    entry_price = 0
    entry_btc_price = 0
    trade_direction = 0  # 1 为做多山寨币，-1 为做空山寨币
    entry_beta = 0
    hold_count = 0
    entry_turnover = 0
    entry_z = 0

    # 转为 numpy 数组加速遍历
    timestamps = df.index
    closes = df['close'].values
    btc_closes = df['btc_close'].values
    z_scores = df['z_score'].values
    betas = df['beta_shifted'].values
    turnovers = df['avg_turnover_30d'].values

    for i in range(len(df)):
        z = z_scores[i]

        # 数据不足时不操作
        if np.isnan(z) or np.isnan(betas[i]):
            continue

        # --- 冷却期判断 ---
        if cooldown:
            if -2 <= z <= 2:
                cooldown = False  # Z值回归正常区间，解除冷却
            continue  # 在回归正常区间之前，不允许新开仓

        # --- 出场逻辑 (固定期限) ---
        if in_position:
            hold_count += 1
            if hold_count >= Config.HOLDING_PERIOD_HOURS:
                # 触发绝对时间退出
                exit_price = closes[i]
                exit_btc_price = btc_closes[i]

                # 计算收益 (以名义本金 1 为基准)
                # 不做空头成本借币利息的复杂处理，简单以涨跌幅计算
                alt_return = (exit_price - entry_price) / entry_price if trade_direction == 1 else (
                                                                                                               entry_price - exit_price) / entry_price

                # 对冲腿收益：做多山寨则做空BTC，做空山寨则做多BTC。权重为 entry_beta
                btc_direction = -trade_direction
                btc_return = (exit_btc_price - entry_btc_price) / entry_btc_price if btc_direction == 1 else (
                                                                                                                         entry_btc_price - exit_btc_price) / entry_btc_price

                gross_pnl = alt_return * 1 + btc_return * abs(entry_beta)

                # 扣除双腿的手续费与滑点成本 (进出各算一次)
                alt_cost = 1 * Config.FEE_RATE * 2
                btc_cost = abs(entry_beta) * Config.FEE_RATE * 2
                total_cost = alt_cost + btc_cost

                net_pnl = gross_pnl - total_cost

                # 实时查表，动态打标
                current_market_median = market_median_series.get(entry_time, np.nan)
                if pd.notna(current_market_median) and entry_turnover >= current_market_median:
                    vol_group = "High_Vol"
                else:
                    vol_group = "Low_Vol"

                # 记录交易
                trades.append({
                    "symbol": symbol,
                    "entry_time": entry_time,
                    "exit_time": timestamps[i],
                    "direction": "LONG_ALT" if trade_direction == 1 else "SHORT_ALT",
                    "entry_price": entry_price,
                    "exit_price": exit_price,
                    "btc_entry": entry_btc_price,
                    "btc_exit": exit_btc_price,
                    "beta": entry_beta,
                    "z_score": entry_z,
                    "avg_turnover_30d": entry_turnover,
                    "market_median_at_entry": current_market_median,
                    "vol_group": vol_group,
                    "gross_pnl": gross_pnl,
                    "total_cost": total_cost,
                    "net_pnl": net_pnl,
                    "holding_hours": hold_count
                })

                # 重置状态，进入冷却期
                in_position = False
                cooldown = True
            continue

        # --- 进场逻辑 ---
        if not in_position and not cooldown:
            if z < -Config.Z_SCORE_THRESHOLD:
                # 超跌，做多山寨，做空BTC
                trade_direction = 1
                in_position = True
            elif z > Config.Z_SCORE_THRESHOLD:
                # 超涨，做空山寨，做多BTC
                trade_direction = -1
                in_position = True

            if in_position:
                entry_time = timestamps[i]
                # 按照要求，简化采用当前 close 价格
                entry_price = closes[i]
                entry_btc_price = btc_closes[i]
                entry_beta = betas[i]
                entry_z = z
                entry_turnover = turnovers[i]
                hold_count = 0

    return pd.DataFrame(trades)


# ==========================================
# 4. 主控调度流程
# ==========================================
def process_symbol(symbol, btc_df, market_median_series):
    """处理单个币种的全流程并保存CSV"""
    output_csv = os.path.join(Config.OUTPUT_DIR, f"{symbol}_trades.csv")

    # 如果已经回测过，跳过 (方便随时中断重启)
    if os.path.exists(output_csv):
        return

    try:
        # 这里替换为您实际的读取逻辑
        kline_path = os.path.join(Config.DATA_DIR, f"{symbol}_1h_2021-01-01_merged.csv")
        if not os.path.exists(kline_path):
            return

        df_alt = pd.read_csv(kline_path)
        # 指定 unit='ms' 将毫秒级时间戳转为 datetime 格式
        df_alt['open_time'] = pd.to_datetime(df_alt['open_time'], unit='ms')
        df_alt.set_index('open_time', inplace=True)

        # 数据对齐，只保留同时存在的时间段
        aligned_alt, aligned_btc = df_alt.align(btc_df, join='inner', axis=0)

        # 数据长度校验：至少需要 31 天 (31 * 24 = 744 根线)
        if len(aligned_alt) < (Config.BETA_WINDOW_HOURS + Config.SIGNAL_WINDOW_HOURS):
            return

        # 计算指标
        df_indicators = calculate_indicators(aligned_alt, aligned_btc)

        # 运行单币种回测
        trade_records = backtest_single_symbol(symbol, df_indicators, market_median_series)

        # 保存到CSV
        if not trade_records.empty:
            trade_records.to_csv(output_csv, index=False)

    except Exception as e:
        print(f"Error processing {symbol}: {e}")


def run_all_backtests(symbols):
    """运行所有币种回测"""
    # 提前预计算全市场的截面中位数水位线
    market_median_series = generate_market_median(symbols)

    print("加载 BTC 基准数据...")
    btc_path = os.path.join(Config.DATA_DIR, f"{Config.BTC_SYMBOL}_1h_2021-01-01_merged.csv")
    btc_df = pd.read_csv(btc_path)

    # 指定 unit='ms' 将毫秒级时间戳转为 datetime 格式
    btc_df['open_time'] = pd.to_datetime(btc_df['open_time'], unit='ms')
    btc_df.set_index('open_time', inplace=True)

    print(f"开始回测配对交易，共 {len(symbols)} 个币种...")
    for symbol in tqdm(symbols):
        if symbol == Config.BTC_SYMBOL:
            continue
        process_symbol(symbol, btc_df, market_median_series)

# ==========================================
# 5. 统计与分析模块 (第四、五步：分组与留出期检验基础)
# ==========================================
def analyze_results():
    """汇总所有保存的 CSV 交易记录并生成四象限统计信息"""
    all_files = glob.glob(os.path.join(Config.OUTPUT_DIR, "*_trades.csv"))
    if not all_files:
        print("没有找到任何交易记录文件。")
        return

    df_all_trades = pd.concat((pd.read_csv(f) for f in all_files), ignore_index=True)

    if df_all_trades.empty:
        print("交易记录为空。")
        return

    # 打印整体表现
    print("\n" + "=" * 40)
    print("【全局回测表现 (已扣除手续费与滑点)】")
    print(f"总交易次数: {len(df_all_trades)}")
    print(f"胜率: {(df_all_trades['net_pnl'] > 0).mean():.2%}")
    print(f"单笔平均净收益率: {df_all_trades['net_pnl'].mean():.4%}")
    print(f"累计净收益: {df_all_trades['net_pnl'].sum():.2f} 单位本金")
    print("=" * 40)

    # 纯靠记录内自带的 vol_group 进行四象限独立核算，非常干净
    quadrants = df_all_trades.groupby(['vol_group', 'direction']).agg(
        trade_count=('symbol', 'count'),
        win_rate=('net_pnl', lambda x: (x > 0).mean()),
        avg_net_pnl=('net_pnl', 'mean'),
        sum_net_pnl=('net_pnl', 'sum'),
        avg_cost=('total_cost', 'mean')
    ).reset_index()

    print("\n【四象限独立核算表现】")
    print(quadrants.to_string(index=False))


# ==========================================
# 启动入口
# ==========================================
if __name__ == "__main__":
    from common.common_utils import read_json

    # 读取币种列表
    SYMBOLS = read_json("symbols.json")

    # 步骤 1：遍历执行回测并落地 CSV (若存在则自动跳过)
    run_all_backtests(SYMBOLS)

    # 步骤 2：加载所有交易记录进行统计分析
    analyze_results()