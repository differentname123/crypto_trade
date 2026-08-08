# -*- coding: utf-8 -*-
import pandas as pd
import uuid
import os
from datetime import datetime


def inject_manual_positions(ledger_file="trade_records_top_long.csv"):
    COLUMNS = [
        "record_id", "signal_time", "strategy_name", "symbol", "direction", "event",
        "client_oid", "exchange_oid",
        "target_amount", "filled_amount", "actual_fill_price", "target_value",
        "exec_status", "linked_open_id", "update_time", "error_msg"
    ]

    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    # 注意：这里使用 CCXT 标准的币安 U 本位合约交易对格式 (BASE/QUOTE:SETTLE)
    # 必须与你策略发出的信号 symbol 完全一致，否则无法匹配隔离单元 (ssd_key)
    manual_positions = [
        {
            "symbol": "BLESS/USDT:USDT",
            "direction": "LONG",
            "filled_amount": 388,
            "actual_fill_price": 0.0129000,
        },
        {
            "symbol": "MMT/USDT:USDT",
            "direction": "LONG",
            "filled_amount": 23,
            "actual_fill_price": 0.2269999,
        }
    ]

    new_rows = []
    for pos in manual_positions:
        row = {col: "" for col in COLUMNS}
        # 伪造核心逻辑：欺骗 _find_open_to_close
        row["record_id"] = uuid.uuid4().hex  # 生成全局唯一ID
        row["signal_time"] = now_str
        row["strategy_name"] = "DEF"  # 默认策略名，需与 parse_signal 中的默认值一致
        row["symbol"] = pos["symbol"]
        row["direction"] = pos["direction"]
        row["event"] = "OPEN"  # 必须是 OPEN，留给后续 CLOSE 去寻找
        row["client_oid"] = f"manual_{uuid.uuid4().hex[:8]}"
        row["exchange_oid"] = "manual_entry"
        row["target_amount"] = pos["filled_amount"]
        row["filled_amount"] = pos["filled_amount"]  # 必须 > 0
        row["actual_fill_price"] = pos["actual_fill_price"]
        row["exec_status"] = "FILLED"  # 状态必须是 FILLED
        row["update_time"] = now_str
        row["error_msg"] = "手动建仓注入记录"
        new_rows.append(row)

    new_df = pd.DataFrame(new_rows, columns=COLUMNS)

    # 读取并追加到现有账本，或创建新账本
    if os.path.exists(ledger_file):
        existing_df = pd.read_csv(
            ledger_file,
            dtype={"record_id": str, "client_oid": str, "exchange_oid": str, "linked_open_id": str}
        )
        # 补齐可能缺失的列
        for col in COLUMNS:
            if col not in existing_df.columns:
                existing_df[col] = ""
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
    else:
        final_df = new_df

    # 原子写入，与 LedgerManager 保持同样的安全性
    tmp_path = ledger_file + ".tmp"
    final_df.to_csv(tmp_path, index=False, encoding="utf-8")
    os.replace(tmp_path, ledger_file)

    print(f"✅ 成功将 {len(manual_positions)} 条手动仓位注入到 {ledger_file} 账本中！")
    for pos in manual_positions:
        print(f"   -> 注入成功: {pos['symbol']} | {pos['direction']} | 数量: {pos['filled_amount']}")


if __name__ == "__main__":
    inject_manual_positions()