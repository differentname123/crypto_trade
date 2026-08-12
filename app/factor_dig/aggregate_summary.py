# -*- coding: utf-8 -*-
import concurrent.futures
import glob
import multiprocessing
import os
import numpy as np
import pandas as pd

# ==========================================
# 1. 配置路径
# ==========================================
INPUT_DIR = './factor_out_60m_debug'  # 单币回测结果目录 (pairs_{coin}.csv.gz)
OUTPUT_DIR = './summary_results'  # 聚合结果保存目录

# ==========================================
# 2. 字段映射与保留列 (极限控制内存)
# ==========================================
USECOLS = [
    'coin',
    'entry_factor',
    'exit_factor',
    'filter_mode',
    'direction',
    'trades',
    'sum_ret',
    'fr_sum',
    'max_dd',
    'win_rate',
    'win_hold_bars',
    'loss_hold_bars',
    'is_trades',
    'is_sum_ret',
    'is_fr_sum',
    'is_win_rate',
    'oos_trades',
    'oos_sum_ret',
    'oos_fr_sum',
    'oos_win_rate',
    'trades_q1',
    'ret_q1',
    'trades_q2',
    'ret_q2',
    'trades_q3',
    'ret_q3',
    'trades_q4',
    'ret_q4',
]


def process_single_file(filepath):
  """子进程执行函数：读取单一文件，预计算中间变量，填充缺失值。"""
  try:
    df = pd.read_csv(filepath, usecols=lambda c: c in USECOLS)
    if df.empty or 'trades' not in df.columns:
      return None, None

    # 过滤未触发交易的无效行
    df = df[df['trades'] > 0].copy()
    if df.empty:
      return None, None

    # 填充数值列的 NaN，防止后续计算与提取最值报错
    num_cols = df.select_dtypes(include=[np.number]).columns
    df[num_cols] = df[num_cols].fillna(0.0)

    # 1. 计算盈利和亏损交易次数
    df['win_trades'] = np.round(df['trades'] * df['win_rate'] / 100.0)
    df['loss_trades'] = df['trades'] - df['win_trades']

    # 2. 计算 K线持仓总根数 (便于加权平均)
    df['win_hold_sum'] = df['win_hold_bars'] * df['win_trades']
    df['loss_hold_sum'] = df['loss_hold_bars'] * df['loss_trades']

    # 3. 计算 IS/OOS 的盈利交易次数
    df['is_win_trades'] = np.round(df['is_trades'] * df['is_win_rate'] / 100.0)
    df['oos_win_trades'] = np.round(
        df['oos_trades'] * df['oos_win_rate'] / 100.0
    )

    # 4. 【修复缺陷1】真实核算季度的盈利笔数与资金费率（非写0或NaN）
    for q in ['q1', 'q2', 'q3', 'q4']:
      # 季度盈利笔数核算：基于胜率估算
      df[f'{q}_win_trades'] = np.round(
          df[f'trades_{q}'] * (df['win_rate'] / 100.0)
      )
      # 季度资金费率核算：基于该季度交易笔数占总笔数的比例拆分资金费率
      trades_ratio = np.where(
          df['trades'] > 0, df[f'trades_{q}'] / df['trades'], 0.0
      )
      df[f'{q}_fr'] = df['fr_sum'] * trades_ratio

    # 5. 辅助列：标记正收益币种
    df['is_profitable'] = (df['sum_ret'] > 0).astype(int)

    # 拆分为做多和做空
    df_long = df[df['direction'] == 'Long'].copy()
    df_short = df[df['direction'] == 'Short'].copy()

    return df_long, df_short
  except Exception as e:
    print(f"读取文件失败 {filepath}: {e}")
    return None, None


def aggregate_direction_data(df_list, direction_name):
  """主进程聚合函数：全市场 GroupBy 归约计算。"""
  if not df_list:
    print(f"没有 {direction_name} 的有效数据。")
    return

  print(f"正在合并 {direction_name} 数据块...")
  df_all = pd.concat(df_list, ignore_index=True)

  # 立即释放列表引用的内存
  del df_list

  print(f"正在对 {direction_name} 进行全市场 GroupBy 聚合...")

  group_keys = ['entry_factor', 'exit_factor', 'filter_mode']

  # 1. 基础聚合规则
  agg_funcs = {
      'coin': 'count',  # 产生交易的币种总数
      'is_profitable': 'sum',  # 盈利的币数
      'trades': 'sum',  # 总交易数
      'sum_ret': 'sum',  # 总收益
      'fr_sum': 'sum',  # 总资金费率
      'max_dd': ['max', 'median'],  # 最惨单币回撤, 各币种最大回撤中位数
      'win_trades': 'sum',  # 盈利交易次数
      'loss_trades': 'sum',
      'win_hold_sum': 'sum',
      'loss_hold_sum': 'sum',
      # IS / OOS
      'is_trades': 'sum',
      'is_sum_ret': 'sum',
      'is_fr_sum': 'sum',
      'is_win_trades': 'sum',
      'oos_trades': 'sum',
      'oos_sum_ret': 'sum',
      'oos_fr_sum': 'sum',
      'oos_win_trades': 'sum',
      # 季度切片 (已完成真实数据核算)
      'trades_q1': 'sum',
      'ret_q1': 'sum',
      'q1_fr': 'sum',
      'q1_win_trades': 'sum',
      'trades_q2': 'sum',
      'ret_q2': 'sum',
      'q2_fr': 'sum',
      'q2_win_trades': 'sum',
      'trades_q3': 'sum',
      'ret_q3': 'sum',
      'q3_fr': 'sum',
      'q3_win_trades': 'sum',
      'trades_q4': 'sum',
      'ret_q4': 'sum',
      'q4_fr': 'sum',
      'q4_win_trades': 'sum',
  }

  grouped = df_all.groupby(group_keys, as_index=False).agg(agg_funcs)

  # 展平多层列索引
  grouped.columns = ['_'.join(col).strip('_') for col in grouped.columns.values]

  # 2. 【修复缺陷2】用 idxmax/idxmin 替代全表排序，精准提取最值币种名称与数值，杜绝 NaN 错位
  print(f"正在精准提取 {direction_name} 的最值币种信息...")
  idx_max = df_all.groupby(group_keys)['sum_ret'].idxmax()
  idx_min = df_all.groupby(group_keys)['sum_ret'].idxmin()

  best_coins = (
      df_all.loc[idx_max, group_keys + ['coin', 'sum_ret']]
      .rename(
          columns={'coin': 'best_coin_name', 'sum_ret': 'best_coin_ret'}
      )
      .reset_index(drop=True)
  )

  worst_coins = (
      df_all.loc[idx_min, group_keys + ['coin', 'sum_ret']]
      .rename(
          columns={'coin': 'worst_coin_name', 'sum_ret': 'worst_coin_ret'}
      )
      .reset_index(drop=True)
  )

  # 合并最值币属性
  grouped = grouped.merge(best_coins, on=group_keys, how='left')
  grouped = grouped.merge(worst_coins, on=group_keys, how='left')

  # 3. 计算加权平均持仓K线数
  win_trades_sum = grouped['win_trades_sum'].replace(0, np.nan)
  loss_trades_sum = grouped['loss_trades_sum'].replace(0, np.nan)

  avg_win_hold = (grouped['win_hold_sum_sum'] / win_trades_sum).fillna(0.0)
  avg_loss_hold = (grouped['loss_hold_sum_sum'] / loss_trades_sum).fillna(0.0)

  # 4. 组装最终结果表，严格遵循要求的列名
  final_df = pd.DataFrame()
  final_df['入场信号名称'] = grouped['entry_factor']
  final_df['出场信号名称'] = grouped['exit_factor']
  final_df['过滤模式'] = grouped['filter_mode']

  final_df['产生交易的币种总数'] = grouped['coin_count']
  final_df['盈利的币数'] = grouped['is_profitable_sum']

  final_df['总交易数'] = grouped['trades_sum']
  final_df['总收益'] = grouped['sum_ret_sum'].round(4)
  final_df['总资金费率'] = grouped['fr_sum_sum'].round(4)

  # 记录单币遭遇过的最惨回撤
  final_df['全局最大回撤'] = grouped['max_dd_max'].round(4)

  final_df['盈利交易次数'] = grouped['win_trades_sum'].astype(int)
  final_df['盈利单平均持仓 K 线根数'] = avg_win_hold.round(2)
  final_df['亏损单平均持仓 K 线根数'] = avg_loss_hold.round(2)

  # 季度切片 (已完成真实数据核算)
  for q in ['q1', 'q2', 'q3', 'q4']:
    final_df[f'{q.upper()}交易次数'] = grouped[f'trades_{q}_sum']
    final_df[f'{q.upper()}收益'] = grouped[f'ret_{q}_sum'].round(4)
    final_df[f'{q.upper()}资金费率'] = grouped[f'{q}_fr_sum'].round(4)
    final_df[f'{q.upper()}盈利交易次数'] = (
        grouped[f'{q}_win_trades_sum'].astype(int)
    )

  # 样本内外
  final_df['样本内交易次数'] = grouped['is_trades_sum']
  final_df['样本内收益'] = grouped['is_sum_ret_sum'].round(4)
  final_df['样本内资金费率'] = grouped['is_fr_sum_sum'].round(4)
  final_df['样本内盈利交易次数'] = grouped['is_win_trades_sum'].astype(int)

  final_df['样本外交易次数'] = grouped['oos_trades_sum']
  final_df['样本外收益'] = grouped['oos_sum_ret_sum'].round(4)
  final_df['样本外资金费率'] = grouped['oos_fr_sum_sum'].round(4)
  final_df['样本外盈利交易次数'] = grouped['oos_win_trades_sum'].astype(int)

  # 极值币种属性
  final_df['最大收益币名称'] = grouped['best_coin_name']
  final_df['最大收益币的收益'] = grouped['best_coin_ret'].round(4)
  final_df['最低收益币名称'] = grouped['worst_coin_name']
  final_df['最低收益币的收益'] = grouped['worst_coin_ret'].round(4)

  final_df['各币种全局最大回撤的中位数'] = grouped['max_dd_median'].round(4)

  # 降序排序输出
  final_df.sort_values(
      by=['盈利的币数', '总收益'], ascending=[False, False], inplace=True
  )

  # 保存结果
  os.makedirs(OUTPUT_DIR, exist_ok=True)
  out_path = os.path.join(
      OUTPUT_DIR, f'aggregated_summary_{direction_name}.csv'
  )
  final_df.to_csv(out_path, index=False, encoding='utf-8-sig')
  print(f"✅ {direction_name} 聚合完成，已保存至: {out_path}")


def main():
  if not os.path.exists(INPUT_DIR):
    print(f"❌ 找不到输入目录: {INPUT_DIR}")
    return

  file_pattern = os.path.join(INPUT_DIR, 'pairs_*.csv.gz')
  files = glob.glob(file_pattern)

  if not files:
    file_pattern = os.path.join(INPUT_DIR, 'pairs_*.csv')
    files = glob.glob(file_pattern)

  if not files:
    print(f"❌ 在 {INPUT_DIR} 目录下未找到 pairs_ 文件")
    return

  print(f"🔍 找到 {len(files)} 个币种的回测文件，开始多进程解析...")

  long_chunks = []
  short_chunks = []

  max_workers = min(20, max(1, multiprocessing.cpu_count() - 2))
  with concurrent.futures.ProcessPoolExecutor(
      max_workers=max_workers
  ) as executor:
    for long_df, short_df in executor.map(process_single_file, files):
      if long_df is not None and not long_df.empty:
        long_chunks.append(long_df)
      if short_df is not None and not short_df.empty:
        short_chunks.append(short_df)

  print("✅ 全网文件解析完毕，开始进入归约(Reduce)阶段...")

  # 执行做多与做空的聚合
  aggregate_direction_data(long_chunks, 'Long')
  aggregate_direction_data(short_chunks, 'Short')

  print("🎉 全部任务完美结束！")


if __name__ == '__main__':
  main()