import os
import glob
import time
from datetime import datetime
import pandas as pd


def log(msg: str):
    """标准的日志打印输出格式"""
    current_time = datetime.now().strftime('%H:%M:%S')
    print(f"[{current_time}] {msg}")


def add_cross_sectional_rank_stable_with_logs(data_dir: str):
    """
    最稳定版：为所有的 1m 级别 K 线数据增加 24H 截面涨跌幅排名 (带详细监控日志)
    注意：此版本将直接在原始数据文件上进行修改和字段追加，原地覆盖。
    """
    global_start_time = time.time()

    kfiles = glob.glob(os.path.join(data_dir, '*_USDT_USDT_1m_kline.csv'))
    total_files = len(kfiles)

    if total_files == 0:
        log("❌ 未找到任何符合 *_USDT_USDT_1m_kline.csv 规则的文件，程序退出。")
        return

    log(f"🚀 开始执行横截面特征工程，共发现 {total_files} 个目标文件。")
    log(f"📂 数据源目录 (将直接在原文件上修改): {os.path.abspath(data_dir)}")
    print("-" * 60)

    # ==========================================
    # 阶段一：提取数据构建对齐网格
    # ==========================================
    log("▶️ [阶段 1/3] 正在提取所有币种的收盘价与时间戳...")
    step1_start = time.time()

    close_dict = {}
    time_col_map = {}

    for i, file_path in enumerate(kfiles, 1):
        coin = os.path.basename(file_path).split('_USDT')[0]

        # 仅读取必需列以节约内存
        df = pd.read_csv(file_path, usecols=lambda c: c in ['timestamp', 'open_time', 'time', 'ts', 'close'])
        t_col = [c for c in df.columns if c != 'close'][0]
        time_col_map[coin] = t_col

        # 统一转为标准 datetime，屏蔽毫秒/秒差异
        unit = 'ms' if df[t_col].iloc[0] > 1e11 else 's'
        df['dt'] = pd.to_datetime(df[t_col], unit=unit, utc=True)

        df = df.drop_duplicates(subset=['dt']).set_index('dt')
        close_dict[coin] = df['close']

        # 【关键日志】每 20 个币种播报一次读取进度
        if i % 20 == 0 or i == total_files:
            log(f"  -> 已成功读取 {i}/{total_files} 个文件...")

    panel = pd.DataFrame(close_dict).sort_index()
    log(f"✅ [阶段 1 完成] 宽表构建完毕！当前宽表维度 (行x列): {panel.shape}，耗时: {time.time() - step1_start:.2f}s")
    print("-" * 60)

    # ==========================================
    # 阶段二：重采样处理断层并计算排名
    # ==========================================
    log("▶️ [阶段 2/3] 正在执行严格的 1min 重采样对齐及收益率计算...")
    step2_start = time.time()

    log("  -> 正在对齐时间网格(ffill)，修正交易所K线断层 (此步计算密集，请稍候)...")
    panel = panel.resample('1min').ffill(limit=720)
    log(f"  -> 重采样完成！填充后的严谨宽表维度: {panel.shape}")

    log("  -> 正在计算 1440 根 (24小时) 滚动收益率...")
    ret_24h = panel.pct_change(periods=1440)

    log("  -> 正在计算横截面涨跌幅名次 (忽略未上市及无交易状态的 NaN)...")
    rank_gain = ret_24h.rank(axis=1, ascending=False, method='min')
    rank_loss = ret_24h.rank(axis=1, ascending=True, method='min')

    log(f"✅ [阶段 2 完成] 全市场截面特征矩阵计算完毕！耗时: {time.time() - step2_start:.2f}s")
    print("-" * 60)

    # ==========================================
    # 阶段三：回写数据并落盘 (原子级绝对等幂版)
    # ==========================================
    log("▶️ [阶段 3/3] 正在将截面特征合并回原文件并直接覆盖...")
    step3_start = time.time()

    for i, file_path in enumerate(kfiles, 1):
        file_start = time.time()
        file_name = os.path.basename(file_path)
        coin = file_name.split('_USDT')[0]
        t_col = time_col_map[coin]

        # 读全量数据
        df_orig = pd.read_csv(file_path)

        # 【微调 1：清洗历史特征，保障原地覆盖等幂】
        # 如果表里已经有这两个列（之前跑过的），先无情删掉，保证每次都是干净的 Join
        cols_to_clean = ['rank_gain_24h', 'rank_loss_24h']
        existing_cols = [c for c in cols_to_clean if c in df_orig.columns]
        if existing_cols:
            df_orig.drop(columns=existing_cols, inplace=True)

        # 匹配桥梁
        unit = 'ms' if df_orig[t_col].iloc[0] > 1e11 else 's'
        df_orig['dt_match'] = pd.to_datetime(df_orig[t_col], unit=unit, utc=True)

        ranks = pd.DataFrame({
            'rank_gain_24h': rank_gain[coin].values,
            'rank_loss_24h': rank_loss[coin].values
        }, index=rank_gain.index)

        # 无误差的 Left Join
        df_orig = df_orig.merge(ranks, left_on='dt_match', right_index=True, how='left')
        df_orig.drop(columns=['dt_match'], inplace=True)

        # 【微调 2：原子级写盘（Atomic Write），防止断电产生半截残缺文件】
        # 变更点：此处 out_file 直接指向原始的 file_path
        out_file = file_path
        tmp_file = out_file + ".tmp"  # 先写到临时文件
        df_orig.to_csv(tmp_file, index=False)
        os.replace(tmp_file, out_file)  # 瞬间重命名覆盖原文件（系统底层原子操作，绝不会损坏原数据）

        log(f"  -> [{i}/{total_files}] 成功更新原文件: {coin:<10} | 耗时: {time.time() - file_start:.2f}s")

    log(f"✅ [阶段 3 完成] 所有文件原地写入更新完毕！耗时: {time.time() - step3_start:.2f}s")
    print("=" * 60)

    # ==========================================
    # 总结与体检
    # ==========================================
    total_cost = time.time() - global_start_time
    log(f"🎉 全部处理流程圆满结束！")
    log(f"📊 累计处理文件: {total_files} 个")
    log(f"⏱️ 任务总耗时: {total_cost:.2f} 秒 (约 {total_cost / 60:.1f} 分钟)")
    log(f"📁 最终数据已在原目录更新: {os.path.abspath(data_dir)}")


def check_rank_fields_exist(data_dir: str):
    """
    极速扫描：判断目录下的 K 线文件中，哪些已经包含了截面排名特征字段。

    参数:
        data_dir (str): 数据目录路径

    返回:
        Tuple[List[str], List[str]]: 返回一个元组，包含两个列表：
            - processed_files: 包含目标字段的文件路径列表
            - unprocessed_files: 不包含目标字段的文件路径列表
    """
    kfiles = glob.glob(os.path.join(data_dir, '*_USDT_USDT_1m_kline.csv'))
    total_files = len(kfiles)

    if total_files == 0:
        log("❌ 未找到任何目标文件。")
        return [], []

    log(f"🔍 开始扫描 {total_files} 个文件，检查特征字段是否存在...")

    # 需要检查的目标字段
    target_cols = {'rank_gain_24h', 'rank_loss_24h'}

    processed_files = []
    unprocessed_files = []

    start_time = time.time()

    for i, file_path in enumerate(kfiles, 1):
        # 【核心优化】：nrows=0 意味着只读取 CSV 的表头，不加载数据体，速度极快
        df_header = pd.read_csv(file_path, nrows=0)
        current_cols = set(df_header.columns)

        # 判断目标字段是否是当前表头的子集（即是否全部包含）
        if target_cols.issubset(current_cols):
            processed_files.append(file_path)
        else:
            unprocessed_files.append(file_path)

        # 进度打印
        if i % 50 == 0 or i == total_files:
            log(f"  -> 已扫描 {i}/{total_files} 个文件...")

    cost_time = time.time() - start_time

    # 打印统计结果
    print("=" * 60)
    log("📊 扫描结果统计：")
    log(f"✅ 已包含目标字段的文件数: {len(processed_files)} 个")
    log(f"❌ 未包含目标字段的文件数: {len(unprocessed_files)} 个")
    log(f"⏱️ 扫描总耗时: {cost_time:.2f} 秒")
    print("=" * 60)

    return processed_files, unprocessed_files

# df = pd.read_csv(r'W:\project\python_project\crypto_trade\app\factor_dig\factor_out_60m\pairs_1INCH.csv')
# ================= 调用示例 =================
# 现在只需要传入包含 CSV 原始数据的目录路径即可

check_rank_fields_exist('../data')
# add_cross_sectional_rank_stable_with_logs('../data')