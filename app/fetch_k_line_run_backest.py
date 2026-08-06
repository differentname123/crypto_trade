import pandas as pd
import numpy as np
import datetime

# ==========================================
# 1. 策略核心参数区 (方便后续调整)
# ==========================================
OI_WINDOW_DAYS = 30         # OI 历史分位数统计周期（天）
OI_PERCENTILE = 0.90        # OI 极端拥挤水位线（90%分位数）
FR_THRESHOLD = -0.0005      # 空头流血阈值，即 -0.05%
BREAKOUT_WINDOW_H = 12      # 进场突破计算周期（小时），过去 12 小时高点
STOPLOSS_WINDOW_H = 1       # 出场止损计算周期（小时），过去 1 小时低点
FEE_RATE = 0.001            # 单边交易成本（0.1%）

target_coin = 'DEXE'  # 回测目标币种 (可修改为其他币种，如 ETH、BNB 等)
# 数据文件路径配置
OI_FILE = f'./data/{target_coin}_USDT_USDT_5m_oi.csv'
FR_FILE = f'./data/{target_coin}_USDT_USDT_funding_rates.csv'
KLINE_FILE = f'./data/{target_coin}_USDT_USDT_1m_kline.csv'

def run_backtest():
    print(">>> 正在加载并预处理数据...")
    
    # ------------------------------------------
    # 2. 数据读取与预处理
    # ------------------------------------------
    # 读取数据
    df_oi = pd.read_csv(OI_FILE)
    df_fr = pd.read_csv(FR_FILE)
    df_klines = pd.read_csv(KLINE_FILE)

    # 确保时间戳排序 (防止原始数据乱序)
    df_oi = df_oi.sort_values('timestamp').reset_index(drop=True)
    df_fr = df_fr.sort_values('timestamp').reset_index(drop=True)
    df_klines = df_klines.sort_values('timestamp').reset_index(drop=True)

    # 将毫秒时间戳转换为 datetime 用于基于时间的滚动计算
    df_oi['datetime'] = pd.to_datetime(df_oi['timestamp'], unit='ms')
    df_klines['datetime'] = pd.to_datetime(df_klines['timestamp'], unit='ms')

    # --- OI 处理 (计算过去 30 天 90% 分位数) ---
    df_oi = df_oi.set_index('datetime')
    # 滚动计算 30D 的 90% 分位数
    df_oi['oi_90pct'] = df_oi['oi_amount'].rolling(f'{OI_WINDOW_DAYS}D').quantile(OI_PERCENTILE)
    # 判定条件A：当前 OI 是否大于 90% 分位数
    df_oi['cond_A'] = df_oi['oi_amount'] > df_oi['oi_90pct']
    df_oi = df_oi.reset_index()

    # --- 资金费率处理 ---
    # 判定条件B：最近一次资金费率是否达标 (-0.05%)
    df_fr['cond_B'] = df_fr['funding_rate'] <= FR_THRESHOLD

    # --- K线处理 (计算通道突破) ---
    df_klines = df_klines.set_index('datetime')
    # 【防未来函数】：把 high 和 low 往下移一格，这样 rolling 计算过去 N 小时最值时，就不会把“当前这根还在走的 K 线”算进去
    df_klines['high_prev'] = df_klines['high'].shift(1)
    df_klines['low_prev'] = df_klines['low'].shift(1)
    # 基于时间滚动计算前 12h 最高 和 前 1h 最低
    df_klines['resist_12h'] = df_klines['high_prev'].rolling(f'{BREAKOUT_WINDOW_H}h').max()
    df_klines['support_1h'] = df_klines['low_prev'].rolling(f'{STOPLOSS_WINDOW_H}h').min()
    df_klines = df_klines.reset_index()

    # ------------------------------------------
    # 3. 跨周期数据对齐 (严防未来函数)
    # ------------------------------------------
    # 使用 merge_asof 按时间向后匹配，确保 1m K线只获取到 "当前时间或之前" 已经生成的 OI 和 FR 数据快照
    # direction='backward' 完美符合实盘逻辑：拿到最近的一次已播报数据
    df_master = pd.merge_asof(
        df_klines, 
        df_oi[['timestamp', 'cond_A', 'oi_amount', 'oi_90pct']], 
        on='timestamp', direction='backward'
    )
    df_master = pd.merge_asof(
        df_master, 
        df_fr[['timestamp', 'cond_B', 'funding_rate']], 
        on='timestamp', direction='backward'
    )
    
    # 剔除前期 rolling 没有计算出数据的 NaN 阶段
    # 【修复】：确保第一笔交易必须在有了完整的30天历史数据之后才开始
    # 获取 OI 数据的最早时间
    first_oi_time = df_oi['datetime'].iloc[0]
    # 计算有效起始时间 = 最早时间 + 30天
    valid_start_time = first_oi_time + pd.Timedelta(days=OI_WINDOW_DAYS)

    # 过滤掉预热期的数据
    df_master = df_master[df_master['datetime'] >= valid_start_time].reset_index(drop=True)

    print(f">>> 剔除30天预热期后，实际用于回测的有效 K 线数量: {len(df_master)}")
    print(f">>> 数据预处理完成，有效 K 线数量: {len(df_master)}")
    print(">>> 开始回测模拟...\n")

    # ------------------------------------------
    # 4. 回测主逻辑 (状态机遍历)
    # ------------------------------------------
    in_position = False
    entry_price = 0.0
    entry_time = None
    entry_fee = 0.0
    
    trades = []
    initial_capital = 10000.0  # 初始模拟资金
    capital = initial_capital
    
    # 我们遍历每一根 K 线。变量 'i' 代表当前刚刚收盘的这根 K 线，'i+1' 是下一根要开盘的 K 线
    for i in range(len(df_master) - 1):
        curr_bar = df_master.iloc[i]
        next_bar = df_master.iloc[i + 1]
        
        # 可读的时间字符串用于日志
        curr_time_str = curr_bar['datetime'].strftime('%Y-%m-%d %H:%M:%S')

        if not in_position:
            # 阶段一 & 阶段二：寻找火药桶并点火
            is_fuel_ready = curr_bar['cond_A']
            is_bleeding = curr_bar['cond_B']
            is_breakout = curr_bar['close'] > curr_bar['resist_12h']
            
            if is_fuel_ready and is_bleeding and is_breakout:
                # 触发买入信号，执行在下一根 K 线的开盘
                entry_price = next_bar['open']
                entry_time = next_bar['datetime']
                in_position = True
                
                # 日志埋点
                print(f"🔥 [点火入场] 触发时间: {curr_time_str} | 执行时间: {entry_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   ┣━ 信号依据: 收盘价 {curr_bar['close']:.4f} 突破 12h阻力 {curr_bar['resist_12h']:.4f}")
                print(f"   ┣━ 燃料池(OI): {curr_bar['oi_amount']} (阈值: {curr_bar['oi_90pct']:.1f})")
                print(f"   ┣━ 空头流血(FR): {curr_bar['funding_rate']*100:.4f}%")
                print(f"   ┗━ 入场价格: {entry_price:.4f}\n")
                
        else:
            # 阶段三：出场信号
            is_breakdown = curr_bar['close'] < curr_bar['support_1h']
            
            if is_breakdown:
                # 触发卖出平仓信号，执行在下一根 K 线的开盘
                exit_price = next_bar['open']
                exit_time = next_bar['datetime']
                
                # 计算盈亏 (考虑双边滑点/成本)
                gross_return = (exit_price - entry_price) / entry_price
                net_return = gross_return - (FEE_RATE * 2)  # 扣除一买一卖的手续费
                
                profit_amount = capital * net_return
                capital += profit_amount
                
                trades.append({
                    'entry_time': entry_time,
                    'entry_price': entry_price,
                    'exit_time': exit_time,
                    'exit_price': exit_price,
                    'net_return': net_return,
                    'capital': capital
                })
                
                in_position = False
                
                # 日志埋点
                print(f"🛑 [破位出场] 触发时间: {curr_time_str} | 执行时间: {exit_time.strftime('%Y-%m-%d %H:%M:%S')}")
                print(f"   ┣━ 信号依据: 收盘价 {curr_bar['close']:.4f} 跌破 1h支撑 {curr_bar['support_1h']:.4f}")
                print(f"   ┣━ 出场价格: {exit_price:.4f}")
                print(f"   ┗━ 单笔净收益: {net_return*100:.2f}% | 当前净值: {capital:.2f}\n")

    # ------------------------------------------
    # 5. 绩效统计
    # ------------------------------------------
    print("==========================================")
    print("📊 纯粹逼空捕获系统 (Pure Squeeze Catcher) 回测报告")
    print("==========================================")
    if len(trades) > 0:
        trades_df = pd.DataFrame(trades)
        total_trades = len(trades_df)
        win_trades = len(trades_df[trades_df['net_return'] > 0])
        win_rate = win_trades / total_trades
        total_return_pct = (capital - initial_capital) / initial_capital
        max_drawdown = (trades_df['capital'].cummax() - trades_df['capital']).max() / trades_df['capital'].cummax().max() if total_trades > 0 else 0
        
        print(f"总交易次数: {total_trades}")
        print(f"胜率: {win_rate*100:.2f}%")
        print(f"总净收益率: {total_return_pct*100:.2f}%")
        print(f"最大回撤 (按单笔收盘后): {max_drawdown*100:.2f}%")
        print(f"平均每笔净收益: {trades_df['net_return'].mean()*100:.2f}%")
    else:
        print("未触发任何交易。请检查数据时间跨度或放宽参数限制。")
    print("==========================================")

if __name__ == "__main__":
    run_backtest()