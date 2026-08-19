import pandas as pd
from pathlib import Path
import os
from concurrent.futures import ProcessPoolExecutor, as_completed


def _compare_single_pair(pairs_file, trades_file, conditions):
    """
    处理单个币种文件对的核心逻辑 (在子进程中运行)
    比较 pairs 表中的 trades 数值，与 trades 表中实际的行数是否一致。
    返回: (状态标签, 币种标识, 打印信息)
    """
    # 提取币种标识，比如 "pairs_BTCUSDT.csv.gz" -> "BTCUSDT"
    coin_name = pairs_file.name.replace('pairs_', '').replace('.csv.gz', '')

    # 1. 检查对应的 trades 文件是否存在
    if not trades_file.exists():
        return "missing_in_origin", coin_name, f"[跳过] {coin_name}: 找不到对应的 {trades_file.name} 文件。"

    try:
        # 2. 分别定义要加载的列
        # pairs 表有 'trades' 列，trades 明细表没有 'trades' 列
        cols_pairs = list(conditions.keys()) + ["trades"]
        cols_trades = list(conditions.keys())

        # 3. 极速加载数据
        df_pairs = pd.read_csv(pairs_file, usecols=cols_pairs)
        df_trades = pd.read_csv(trades_file, usecols=cols_trades)

        # 4. 应用过滤条件 (Pairs)
        mask_pairs = (
                (df_pairs["entry_factor"] == conditions["entry_factor"]) &
                (df_pairs["exit_factor"] == conditions["exit_factor"]) &
                (df_pairs["direction"] == conditions["direction"]) &
                (df_pairs["filter_mode"] == conditions["filter_mode"])
        )
        filtered_pairs = df_pairs[mask_pairs]

        # 提取 pairs 表中记录的交易次数 (通常只有一行，安全起见用 sum 求和并转整数)
        if filtered_pairs.empty:
            pairs_trade_count = 0
        else:
            pairs_trade_count = int(filtered_pairs['trades'].sum())

        # 5. 应用过滤条件 (Trades)
        mask_trades = (
                (df_trades["entry_factor"] == conditions["entry_factor"]) &
                (df_trades["exit_factor"] == conditions["exit_factor"]) &
                (df_trades["direction"] == conditions["direction"]) &
                (df_trades["filter_mode"] == conditions["filter_mode"])
        )
        filtered_trades = df_trades[mask_trades]

        # 计算 trades 明细表中的实际行数
        trades_row_count = len(filtered_trades)

        # 6. 比较数据: Pairs 表记录的次数 vs Trades 表实际过滤出的行数
        if pairs_trade_count == trades_row_count:
            return "consistent", coin_name, None
        else:
            msg = f"[警告] {coin_name}: 交易次数对不上！(Pairs汇总显示: {pairs_trade_count} 次, Trades明细行数: {trades_row_count} 次)"
            return "inconsistent", coin_name, msg

    except Exception as e:
        return "errors", coin_name, f"[错误] 处理 {coin_name} 时发生异常: {e}"


def batch_compare_trades_parallel(target_dir):
    # 改为接收参数传入目标目录，从而复用逻辑
    conditions = {
        "entry_factor": "EXIT_SHORT_SURGE_EXTREME",
        "exit_factor": "FR_LOW_NEG",
        "direction": "Long",
        "filter_mode": "bottom_10"
    }

    results = {
        "consistent": [],
        "inconsistent": [],
        "missing_in_origin": [],
        "errors": []
    }

    # 获取所有 pairs 文件
    pairs_files = list(target_dir.glob('pairs_*.csv.gz'))
    total_files = len(pairs_files)

    print(f"共找到 {total_files} 个 pairs 汇总文件。开始交叉验证 trades 明细...")

    # 自动获取当前机器的 CPU 核心数
    max_workers = max(1, (os.cpu_count() or 4) - 1)
    print(f"启动多进程加速... 正在使用 {max_workers} 个 CPU 核心\n")
    print("-" * 50)

    processed_count = 0

    # 使用进程池并行执行
    with ProcessPoolExecutor(max_workers=max_workers) as executor:
        # 提交任务字典，构造对应的 trades 文件路径
        future_to_file = {}
        for pairs_file in pairs_files:
            # 推导 trades 文件名: pairs_BTCUSDT.csv.gz -> trades_BTCUSDT.csv.gz
            trades_file_name = pairs_file.name.replace('pairs_', 'trades_')
            trades_file = target_dir / trades_file_name

            future = executor.submit(_compare_single_pair, pairs_file, trades_file, conditions)
            future_to_file[future] = pairs_file.name

        # 收集结果
        for future in as_completed(future_to_file):
            processed_count += 1
            status, coin_name, msg = future.result()

            # 分类保存结果
            results[status].append(coin_name)

            # 打印错误或不一致信息
            if msg:
                print(msg)

            # 进度提示
            if processed_count % 50 == 0:
                print(f"进度: {processed_count} / {total_files}...")

    # 打印最终汇总报告 (稍微修改了输出标题以标记当前目录)
    print("\n" + "=" * 50)
    print(f"比对完成！[{target_dir.name}] 内部交叉验证报告：")
    print(f"总计检查文件对: {total_files}")
    print(f"✅ 完全一致: {len(results['consistent'])} 对")
    print(f"❌ 数据不一致: {len(results['inconsistent'])} 对")
    print(f"⚠️ 缺失 Trades 明细: {len(results['missing_in_origin'])} 对")
    print(f"❗ 读取/处理错误: {len(results['errors'])} 对")
    print("=" * 50)


if __name__ == "__main__":
    # 定义基础路径
    base_path = Path(r'W:\project\python_project\crypto_trade\app\factor_dig')

    # 定义需要串行遍历的周期时间
    timeframes = ['60m', '30m', '15m', '5m']

    # 循环对每个目录进行串行检测
    for tf in timeframes:
        target_directory = base_path / f'factor_out_{tf}_debugtest'

        print(f"\n\n{'#' * 60}")
        print(f"开始串行检测目录: {target_directory}")
        print(f"{'#' * 60}")

        # 为了防错，如果某目录不存在则抛出提示并跳过，避免报错退出
        if target_directory.exists():
            batch_compare_trades_parallel(target_directory)
        else:
            print(f"[跳过] 目录不存在: {target_directory}")