# -*- coding: utf-8 -*-
"""
[功能摘要] 跨周期信号交易执行系统：按策略原子单元隔离，通过 REST 快照、CSV 账本对账与时间窗信号执行完成开平仓闭环。
[输入数据] 信号 DataFrame（time/strategy/symbol/direction/event/action/price/weight 等）、账户权益/持仓/挂单快照、策略专属 CSV 账本。
[数据流转/交互] 调度周期 → 账户预加载 → PENDING 订单对账/账实校验 → 策略信号生成 → 时间窗过滤 → SSD 单元 OPEN/CLOSE → 账本落盘 → 盘后对账与汇总。
[输出数据] 产生交易所下单/撤单副作用，持续更新 CSV 账本，并输出聚合后的调度、交易、对账和持仓诊断日志。

设计原则：简单为底 / 账本为核 / 隔离为纲 / 实事求是。
平台无关：会话、字段、错误码、精度、订单解析与 Client OID 规则统一由 binance_u_gateway 适配层承接。
"""

import os
import platform
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta

import pandas as pd

from common_utils import setup_logger
from signal_generator import (
    execute_trading_bot_high_fr_bear_div_short, execute_trading_bot_oi_decay_short,
    execute_trading_bot_vwap_reclaim_long, execute_trading_bot_workflow_XSR_long,
    execute_trading_bot_workflow_bottom_powder_short, execute_trading_bot_workflow_cross,
    execute_trading_bot_workflow_ma_bottom_long, execute_trading_bot_workflow_short_fr,
    execute_trading_bot_workflow_top_long, execute_trading_bot_workflow_vol_fr_long,
)
from binance_u_gateway import (
    OS_CANCELED, OS_FILLED, OS_OPEN, OS_REJECTED, amount_to_precision, build_client_oid,
    cancel_order_by_id, execute_order, extract_order_view, fetch_open_orders_grouped,
    fetch_order_by_id, fetch_positions_map, fetch_recent_orders_map, fetch_total_equity,
    fetch_usdt_swap_changes, is_cancel_target_gone, make_open_order_stub, make_position_key,
    open_session, order_client_oid, order_exchange_oid, position_key_symbol, sync_exchange_time,
)


# =============================================================================
# L0. 配置与常量
# =============================================================================
CURRENT_SYMBOL = "cross"
ACCOUNT_ALIAS = "mama"
BEST_TOP_N = 10
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "bot_data")
LEDGER_FILE = os.path.join(DATA_DIR, f"trade_records_{CURRENT_SYMBOL}.csv")


LEVERAGE = 1
MIN_ORDER_VALUE = 51 if CURRENT_SYMBOL == "cross" else 6
MAX_ORDER_VALUE = 2000.0 if CURRENT_SYMBOL == "cross" else 500
PRELOAD_AHEAD_MIN = 3
SIGNAL_WINDOW_MIN = 1
OPEN_ORDER_TIMEOUT_HOURS = 2
CLOSE_ORDER_TIMEOUT_HOURS = 4
POSITION_DIFF_TOLERANCE = 0.01
API_MAX_RETRY = 3
RECENT_ORDER_LIMIT = 50

ST_PENDING = "PENDING"
ST_FILLED = "FILLED"
ST_CANCELED = "CANCELED"
ST_FAILED = "FAILED"
ST_MANUAL_CLOSED = "MANUAL_CLOSED_NO_POSITION"
EVENT_OPEN = "OPEN"
EVENT_CLOSE = "CLOSE"

logger = setup_logger(app_name=f"{CURRENT_SYMBOL}_trader")
_last_equity = 0.0
_EX_TO_LEDGER_STATUS = {OS_FILLED: ST_FILLED, OS_CANCELED: ST_CANCELED, OS_REJECTED: ST_FAILED}

# Shape: strategy -> (interval_minutes, preload_ahead_minutes, workflow, rank_mode, top_n)
STRATEGY_CONFIGS = {
    "cross": (60, PRELOAD_AHEAD_MIN, execute_trading_bot_workflow_cross, None, None),
    "top_long": (60, PRELOAD_AHEAD_MIN, execute_trading_bot_workflow_top_long, "top", BEST_TOP_N),
    "ma_bottom_long": (5, 0.5, execute_trading_bot_workflow_ma_bottom_long, "bottom", BEST_TOP_N),
    "XSR_long": (30, 1, execute_trading_bot_workflow_XSR_long, "bottom", 10),
    "fr_short": (30, 1, execute_trading_bot_workflow_short_fr, "top", 1),
    "vol_fr_long": (5, 0.5, execute_trading_bot_workflow_vol_fr_long, "bottom", 10),
    "bottom_power_short": (15, 1, execute_trading_bot_workflow_bottom_powder_short, "bottom", 10),
    "oi_decay_short": (30, 1, execute_trading_bot_oi_decay_short, "top", 3),
    "high_fr_bear_div_short": (15, 1, execute_trading_bot_high_fr_bear_div_short, "top", 1),
    "vwap_reclaim_long": (30, 1, execute_trading_bot_vwap_reclaim_long, "bottom", 10),
}


def _log(scope, message, **fields):
    """统一人类可扫描日志：核心字段始终使用 [] 包裹。"""
    detail = " | ".join(f"{key}: [{value}]" for key, value in fields.items())
    return f"[{scope}] {message}" + (f" | {detail}" if detail else "")


# =============================================================================
# L1. 账本管理：文件锁仅覆盖本地读改写临界区，网络操作不持锁
# =============================================================================
class LedgerManager:
    COLUMNS = [
        "record_id", "signal_time", "strategy_name", "symbol", "direction", "event",
        "client_oid", "exchange_oid", "target_amount", "filled_amount", "actual_fill_price",
        "target_value", "exec_status", "linked_open_id", "update_time", "error_msg",
    ]

    def __init__(self, file_path):
        # 新增：提取文件所在目录，如果不存在则自动创建 bot_data 文件夹
        dir_name = os.path.dirname(file_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        self.file_path = file_path
        self.tmp_path = file_path + ".tmp"  # 临时文件会自动跟随在 bot_data 目录下
        self._lock = threading.Lock()

    def _read_unlocked(self):
        """读取账本并补齐标准列；按原设计，文件缺失或读取失败时返回空表。"""
        if not os.path.isfile(self.file_path):
            return pd.DataFrame(columns=self.COLUMNS)
        try:
            df = pd.read_csv(
                self.file_path,
                dtype={"record_id": str, "client_oid": str, "exchange_oid": str, "linked_open_id": str},
            )
        except Exception as exc:
            logger.error(_log(
                "LEDGER/READ", "账本读取失败，按原设计使用空账本继续",
                File=self.file_path, Reason=exc, Hint="检查 CSV 损坏/占用/权限",
            ))
            return pd.DataFrame(columns=self.COLUMNS)
        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df

    def _atomic_write_unlocked(self, df):
        """先写临时文件再原子替换，降低中断写盘对原账本的破坏风险。"""
        df.to_csv(self.tmp_path, index=False, encoding="utf-8")
        os.replace(self.tmp_path, self.file_path)

    def read(self):
        with self._lock:
            return self._read_unlocked()

    def append(self, record):
        """追加标准账本记录。record 核心 Key 与 COLUMNS 对齐。"""
        with self._lock:
            df = self._read_unlocked()
            row = {column: record.get(column, "") for column in self.COLUMNS}
            self._atomic_write_unlocked(pd.concat([df, pd.DataFrame([row])], ignore_index=True))

    def apply_updates(self, updates_map):
        """批量更新后一次落盘。Shape: {record_id: {column: value}}。"""
        if not updates_map:
            return
        with self._lock:
            df = self._read_unlocked()
            record_ids = df["record_id"].astype(str)
            for record_id, fields in updates_map.items():
                mask = record_ids == str(record_id)
                if not mask.any():
                    logger.warning(_log(
                        "LEDGER/UPDATE", "跳过不存在的账本记录",
                        RecordID=record_id, Hint="账本可能被人工修改或版本不一致",
                    ))
                    continue
                for column, value in fields.items():
                    df.loc[mask, column] = value
            self._atomic_write_unlocked(df)


# =============================================================================
# L2. 领域工具：标准化、状态映射与 SSD 隔离视图
# =============================================================================
def to_num(value, default=0.0):
    """安全数值化；无法解析时返回 default。"""
    number = pd.to_numeric(value, errors="coerce")
    return float(number) if pd.notna(number) else default


def _safe_to_datetime(value):
    """安全时间解析；失败返回 None。"""
    try:
        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        return None


def map_exchange_status(exchange_status):
    """平台标准订单态 → 账本态；OPEN/UNKNOWN 保持 PENDING。"""
    return _EX_TO_LEDGER_STATUS.get(str(exchange_status).upper(), ST_PENDING)


def parse_signal(row):
    """
    标准化信号并派生 SSD/持仓 Key 与 Client OID。
    输入核心字段: time/symbol/coin/direction/event/action/price，可选 STRATEGY_NAME/max_weight。
    输出核心 Key: signal_time/strategy_name/symbol/coin/direction/event/action/price/max_weight/
                  ssd_key/pos_key/prefix/client_oid。
    """
    signal_time = pd.to_datetime(row["time"])
    if getattr(signal_time, "tzinfo", None) is not None:
        signal_time = signal_time.tz_localize(None)

    strategy_name = str(row.get("STRATEGY_NAME", "DEF")).strip()
    symbol = str(row["symbol"]).strip()
    coin = str(row["coin"]).strip().upper()
    direction = str(row["direction"]).strip().upper()
    event = str(row["event"]).strip().upper()
    prefix, client_oid = build_client_oid([
        (strategy_name, 6), (coin, 4), (direction, 1), (event, 1),
        (signal_time.strftime("%d%H%M"), 6),
    ])
    return {
        "signal_time": signal_time, "strategy_name": strategy_name, "symbol": symbol, "coin": coin,
        "direction": direction, "event": event, "action": str(row["action"]).strip().lower(),
        "price": float(row["price"]), "max_weight": to_num(row.get("max_weight", 0.1), 0.1),
        "ssd_key": f"{strategy_name}_{symbol}_{direction}", "pos_key": make_position_key(symbol, direction),
        "prefix": prefix, "client_oid": client_oid,
    }


def make_record(sig, target_amount, target_value, status, client_oid, exchange_oid,
                error_msg="", filled_amount="", actual_fill_price="", linked_open_id=""):
    """构造标准账本记录。sig 需 signal_time/strategy_name/symbol/direction/event。"""
    return {
        "record_id": uuid.uuid4().hex,
        "signal_time": sig["signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "strategy_name": sig["strategy_name"], "symbol": sig["symbol"], "direction": sig["direction"],
        "event": sig["event"], "client_oid": client_oid, "exchange_oid": exchange_oid or "",
        "target_amount": target_amount, "filled_amount": filled_amount,
        "actual_fill_price": actual_fill_price, "target_value": target_value, "exec_status": status,
        "linked_open_id": linked_open_id, "update_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "error_msg": error_msg,
    }


def _filter_ssd(df, sig):
    """截取 strategy + symbol + direction 的账本切片。sig 需 strategy_name/symbol/direction。"""
    if df.empty:
        return df
    return df[
        (df["strategy_name"].astype(str).str.strip() == sig["strategy_name"])
        & (df["symbol"].astype(str).str.strip() == sig["symbol"])
        & (df["direction"].astype(str).str.strip().str.upper() == sig["direction"])
    ]


def _closed_open_ids(df):
    """返回已被有效 CLOSE 关联的 OPEN ID；FAILED/CANCELED 平仓不占用关联关系。"""
    if df.empty:
        return set()
    closes = df[
        (df["event"].astype(str).str.strip().str.upper() == EVENT_CLOSE)
        & (~df["exec_status"].astype(str).isin([ST_FAILED, ST_CANCELED]))
    ]
    ids = closes["linked_open_id"].astype(str).str.strip()
    return {value for value in ids if value and value.lower() != "nan"}


def _active_opens(df):
    """返回有实际成交且尚未被有效平仓关联的 OPEN，统一供溯源/校验/汇总使用。"""
    if df.empty:
        return df
    opens = df[df["event"].astype(str).str.strip().str.upper() == EVENT_OPEN]
    if opens.empty:
        return opens
    filled = pd.to_numeric(opens["filled_amount"], errors="coerce").fillna(0)
    return opens[(~opens["record_id"].astype(str).isin(_closed_open_ids(df))) & (filled > 0)]


def _find_open_to_close(ssd_df):
    """取得本 SSD 最近一条有成交且未平仓的 OPEN；订单状态不覆盖成交事实。"""
    active = _active_opens(ssd_df)
    return None if active.empty else active.iloc[-1]


def _has_pending_order(open_order_cache, sig):
    """同信号前缀挂单幂等检查。open_order_cache Shape: {symbol: [order, ...]}。"""
    return any(
        (order_client_oid(order) or "").startswith(sig["prefix"])
        for order in open_order_cache.get(sig["symbol"], [])
    )


def _cache_order(open_order_cache, symbol, exchange_oid, client_oid):
    """成功发单后立即写入本轮内存挂单缓存，封锁同批次重复发单。"""
    open_order_cache.setdefault(symbol, []).append(make_open_order_stub(exchange_oid, client_oid))


# =============================================================================
# L3. 账户预加载与 PENDING 对账
# =============================================================================
def _retry_fetch(label, fetch_fn):
    """账户数据拉取重试；连续失败返回 None，由上层放弃本轮。"""
    for attempt in range(1, API_MAX_RETRY + 1):
        try:
            return fetch_fn()
        except Exception as exc:
            logger.warning(_log(
                "PRELOAD/FETCH", f"{label}拉取失败，将重试",
                Attempt=f"{attempt}/{API_MAX_RETRY}", Reason=exc, Hint="检查网络/接口/会话",
            ))
            time.sleep(1)
    logger.error(_log(
        "PRELOAD/FETCH", f"{label}连续拉取失败",
        Attempts=API_MAX_RETRY, Result="本轮拒绝使用缺失快照交易",
    ))
    return None


def _is_order_timeout(row, now):
    """OPEN/CLOSE 使用不同超时阈值；时间不可解析时按未超时处理。"""
    event = str(row.get("event", "")).strip().upper()
    timeout_hours = OPEN_ORDER_TIMEOUT_HOURS if event == EVENT_OPEN else CLOSE_ORDER_TIMEOUT_HOURS
    signal_time = _safe_to_datetime(row.get("signal_time"))
    return (
        (False, timeout_hours)
        if signal_time is None
        else ((now - signal_time) > timedelta(hours=timeout_hours), timeout_hours)
    )


def reconcile_ledger(exchange, ledger, open_order_cache):
    """
    对 PENDING 订单做三级穿透：活跃挂单缓存 → 按币种近期订单 → 单笔查询；超时订单尝试撤单。
    open_order_cache Shape: {symbol: [平台无关订单对象, ...]}。
    """
    started_at = time.perf_counter()
    df = ledger.read()
    if df.empty:
        return
    pending = df[df["exec_status"].astype(str) == ST_PENDING]
    if pending.empty:
        return

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    updates, synced_count, timeout_count = {}, 0, 0

    open_orders_by_id = {}
    for orders in (open_order_cache or {}).values():
        for order in orders:
            exchange_oid = order_exchange_oid(order)
            if exchange_oid:
                open_orders_by_id[exchange_oid] = order

    missing_symbols = set()
    for _, row in pending.iterrows():
        exchange_oid = str(row.get("exchange_oid", "")).strip()
        if exchange_oid and exchange_oid.lower() != "nan" and exchange_oid not in open_orders_by_id:
            symbol = str(row.get("symbol", "")).strip()
            if symbol:
                missing_symbols.add(symbol)

    recent_orders_by_id = {}
    for symbol in missing_symbols:
        try:
            recent_orders_by_id.update(
                fetch_recent_orders_map(exchange, symbol, limit=RECENT_ORDER_LIMIT)
            )
        except Exception as exc:
            logger.warning(_log(
                "RECON/BATCH", "近期订单批量查询失败，将由单笔查询兜底",
                Symbol=symbol, Reason=exc,
            ))
            time.sleep(0.5)

    for _, row in pending.iterrows():
        record_id = str(row["record_id"])
        exchange_oid = str(row.get("exchange_oid", "")).strip()
        symbol = str(row.get("symbol", "")).strip()
        if not exchange_oid or exchange_oid.lower() == "nan":
            continue

        order_info = open_orders_by_id.get(
            exchange_oid,
            recent_orders_by_id.get(exchange_oid),
        )
        if order_info is None:
            try:
                time.sleep(0.1)
                order_info = fetch_order_by_id(exchange, symbol, exchange_oid)
            except Exception as exc:
                logger.warning(_log(
                    "RECON/LOOKUP", "单笔订单查询失败，本轮暂不改账",
                    RecordID=record_id, ExchangeOID=exchange_oid,
                    Symbol=symbol, Reason=exc,
                ))
                continue
        if not order_info:
            continue

        view = extract_order_view(order_info)
        exchange_status = view["status"]
        filled_amount = to_num(view["filled"])
        avg_price = view["avg_price"]
        is_timeout, timeout_hours = _is_order_timeout(row, now)

        if exchange_status == OS_OPEN and is_timeout:
            event = str(row.get("event", "")).strip().upper()
            final_status = OS_CANCELED
            try:
                cancel_order_by_id(exchange, symbol, exchange_oid)

                try:
                    final_view = extract_order_view(
                        fetch_order_by_id(exchange, symbol, exchange_oid)
                    )
                    final_status = final_view["status"]
                    filled_amount = to_num(final_view["filled"], filled_amount)
                    avg_price = final_view["avg_price"] or avg_price
                except Exception as exc:
                    logger.warning(_log(
                        "RECON/CANCEL",
                        "撤单已提交，但终态复查失败；按已撤单继续核销",
                        ExchangeOID=exchange_oid, Reason=exc,
                    ))

                # 技术修复：
                # 原实现即使复查发现“撤单瞬间已成交”也固定写 CANCELED，
                # 会错误释放已成交 CLOSE 的 linked_open_id；现在优先采用复查终态。
                ledger_status = map_exchange_status(final_status)
                if ledger_status == ST_PENDING:
                    ledger_status = ST_CANCELED

                update = {
                    "exec_status": ledger_status,
                    "update_time": now_str,
                    "error_msg": f"{event}单超时(>{timeout_hours}H)自动撤单",
                }
                if filled_amount > 0:
                    update.update({
                        "filled_amount": filled_amount,
                        "actual_fill_price": avg_price,
                    })

                updates[record_id] = update
                timeout_count += 1
                logger.info(_log(
                    "RECON/CANCEL", "超时订单已处理",
                    Event=event,
                    ExchangeOID=exchange_oid,
                    FinalStatus=ledger_status,
                    Filled=filled_amount,
                ))

            except Exception as exc:
                if is_cancel_target_gone(exc):
                    updates[record_id] = {
                        "exec_status": ST_CANCELED,
                        "update_time": now_str,
                        "error_msg": f"撤单查无此单, 强制核销: {exc}",
                    }
                    timeout_count += 1
                    logger.warning(_log(
                        "RECON/CANCEL",
                        "撤单目标已不存在，按原规则强制核销",
                        ExchangeOID=exchange_oid,
                        Result=ST_CANCELED,
                        Hint="订单可能已被交易所清理或历史查询窗口已过",
                    ))
                else:
                    updates[record_id] = {
                        "update_time": now_str,
                        "error_msg": f"超时撤单失败: {exc}",
                    }
                    logger.warning(_log(
                        "RECON/CANCEL",
                        "超时撤单失败，保留 PENDING 等待下轮",
                        ExchangeOID=exchange_oid,
                        Reason=exc,
                        Hint="检查连接/订单状态/撤单权限",
                    ))
            continue

        ledger_status = map_exchange_status(exchange_status)
        if ledger_status == ST_PENDING and filled_amount <= 0:
            continue

        update = {"update_time": now_str}
        if ledger_status != ST_PENDING:
            update["exec_status"] = ledger_status
        if filled_amount > 0:
            update.update({
                "filled_amount": filled_amount,
                "actual_fill_price": avg_price,
            })

        updates[record_id] = update
        synced_count += 1

    ledger.apply_updates(updates)
    if synced_count or timeout_count:
        logger.info(_log(
            "RECON/SUMMARY",
            "PENDING 对账完成",
            Synced=f"{synced_count}笔",
            TimeoutHandled=f"{timeout_count}笔",
            Elapsed=f"{(time.perf_counter() - started_at) * 1000:.0f}ms",
        ))


def check_position_consistency(ledger, position_cache):
    """
    仅告警账本理论持仓显著大于交易所实际持仓。
    position_cache Shape: {position_key: amount}。
    """
    active_opens = _active_opens(ledger.read())
    if active_opens.empty:
        return

    expected_by_position = {}
    for _, row in active_opens.iterrows():
        position_key = make_position_key(
            str(row["symbol"]).strip(),
            str(row["direction"]).strip(),
        )
        expected_by_position[position_key] = (
            expected_by_position.get(position_key, 0.0)
            + to_num(row["filled_amount"])
        )

    for position_key, expected_amount in expected_by_position.items():
        actual_amount = abs(position_cache.get(position_key, 0.0))
        if actual_amount < expected_amount * (1 - POSITION_DIFF_TOLERANCE):
            logger.warning(_log(
                "RECON/POSITION",
                "账本理论持仓高于交易所实际持仓，不自动调整",
                Position=position_key,
                Ledger=f"{expected_amount:.6f}",
                Exchange=f"{actual_amount:.6f}",
                Tolerance=f"{POSITION_DIFF_TOLERANCE:.2%}",
                Hint="检查手动减仓/外部策略/成交回填",
            ))


def preload_account_state(exchange, ledger):
    """
    拉权益/持仓/挂单 → PENDING 对账 → 持仓一致性检查。
    返回 Shape: (equity, position_cache{key: amount}, open_order_cache{symbol: [order, ...]})。
    """
    global _last_equity

    started_at = time.perf_counter()
    try:
        sync_exchange_time(exchange)
    except Exception as exc:
        logger.warning(_log(
            "PRELOAD/TIME",
            "交易所校时失败，按原设计继续本轮",
            Reason=exc,
            Hint="若后续签名报时间偏差，检查本机时钟/网络延迟",
        ))

    equity = 0.0
    for attempt in range(1, API_MAX_RETRY + 1):
        try:
            equity = fetch_total_equity(exchange)
        except Exception as exc:
            equity = 0.0
            logger.warning(_log(
                "PRELOAD/EQUITY",
                "权益接口异常，将重试",
                Attempt=f"{attempt}/{API_MAX_RETRY}",
                Reason=exc,
            ))

        if equity > 0:
            _last_equity = equity
            break
        time.sleep(1)

    if equity <= 0:
        equity = _last_equity
        logger.warning(_log(
            "PRELOAD/EQUITY",
            "未取得有效权益，沿用上轮缓存",
            CachedEquity=f"{equity:.2f}",
            Impact="缓存也为 0 时本轮最终会被放弃",
        ))

    position_cache = _retry_fetch(
        "持仓",
        lambda: fetch_positions_map(exchange),
    )
    open_order_cache = _retry_fetch(
        "挂单",
        lambda: fetch_open_orders_grouped(exchange),
    )

    if open_order_cache is not None:
        reconcile_ledger(exchange, ledger, open_order_cache)
    if position_cache is not None:
        check_position_consistency(ledger, position_cache)

    position_count = (
        "N/A"
        if position_cache is None
        else len(position_cache)
    )
    order_count = (
        "N/A"
        if open_order_cache is None
        else sum(len(v) for v in open_order_cache.values())
    )

    logger.info(_log(
        "PRELOAD/SUMMARY",
        "账户快照完成",
        Equity=f"{equity:.2f}",
        Positions=position_count,
        OpenOrders=order_count,
        Elapsed=f"{(time.perf_counter() - started_at) * 1000:.0f}ms",
    ))
    return equity, position_cache, open_order_cache


# =============================================================================
# L4. 信号执行：SSD 单元隔离 + 幂等 + 交易所实况封顶
# =============================================================================
def handle_open(
    exchange,
    ledger,
    ledger_df,
    sig,
    total_equity,
    open_order_cache,
):
    """
    执行 OPEN。
    sig 需 ssd_key/symbol/prefix/client_oid/action/direction/price/max_weight。
    """
    ssd_key = sig["ssd_key"]
    ssd_df = _filter_ssd(ledger_df, sig)

    if _has_pending_order(open_order_cache, sig):
        logger.warning(_log(
            "OPEN/SKIP",
            "检测到同信号挂单，阻止重复发单",
            SSD=ssd_key,
            Prefix=sig["prefix"],
        ))
        return

    if not _active_opens(ssd_df).empty:
        logger.warning(_log(
            "OPEN/WARN",
            "本 SSD 仍有未平成交开仓，但按原规则继续执行新信号",
            SSD=ssd_key,
            Hint="检查上一对开平信号或人工干预",
        ))

    target_value = min(
        max(
            total_equity * LEVERAGE * sig["max_weight"],
            MIN_ORDER_VALUE,
        ),
        MAX_ORDER_VALUE,
    )
    amount = amount_to_precision(
        exchange,
        sig["symbol"],
        target_value / sig["price"],
    )

    if amount <= 0:
        logger.warning(_log(
            "OPEN/SKIP",
            "精度处理后下单数量为 0",
            SSD=ssd_key,
            TargetValue=f"{target_value:.2f}",
            Price=sig["price"],
        ))
        return

    result = execute_order(
        exchange=exchange,
        symbol=sig["symbol"],
        side=sig["action"],
        amount=amount,
        client_oid=sig["client_oid"],
        order_type="market",
        reduce_only=False,
        position_side=sig["direction"],
    )
    status = ST_PENDING if result.ok else ST_FAILED

    ledger.append(make_record(
        sig,
        amount,
        target_value,
        status,
        sig["client_oid"],
        result.exchange_oid,
        error_msg=result.error_msg,
    ))

    if result.ok:
        _cache_order(
            open_order_cache,
            sig["symbol"],
            result.exchange_oid,
            sig["client_oid"],
        )

    log_fn = logger.info if result.ok else logger.error
    log_fn(_log(
        "OPEN/ORDER",
        "开仓请求已完成",
        SSD=ssd_key,
        Amount=amount,
        Value=f"{target_value:.2f}",
        Status=status,
        ClientOID=sig["client_oid"],
        **({"Reason": result.error_msg} if result.error_msg else {}),
    ))


def handle_close(
    exchange,
    ledger,
    ledger_df,
    sig,
    position_cache,
    open_order_cache,
):
    """
    执行 CLOSE：平仓量 = min(本 SSD 开仓实际成交量, 交易所该方向实际持仓)。
    sig 需 ssd_key/pos_key/symbol/client_oid/action/direction/price；
    position_cache Shape: {key: amount}。
    """
    ssd_key = sig["ssd_key"]
    open_record = _find_open_to_close(
        _filter_ssd(ledger_df, sig)
    )

    if open_record is None:
        logger.info(_log(
            "CLOSE/SKIP",
            "本 SSD 没有可关联的已成交开仓",
            SSD=ssd_key,
            Result="不发平仓单",
        ))
        return

    linked_open_id = str(open_record["record_id"])
    ledger_open_amount = to_num(open_record["filled_amount"])

    # :
    # position_cache 是本轮预加载快照，同轮先 OPEN 后 CLOSE 时可能尚未反映新成交。
    # 保留原逻辑：快照为 0 时做逻辑核销，避免未经业务确认擅自改变交易边界。
    actual_position = abs(
        position_cache.get(sig["pos_key"], 0.0)
    )

    if actual_position <= 0:
        ledger.append(make_record(
            sig,
            0,
            0,
            ST_MANUAL_CLOSED,
            sig["client_oid"],
            "",
            error_msg="平仓时交易所无持仓, 逻辑核销",
            linked_open_id=linked_open_id,
        ))
        logger.warning(_log(
            "CLOSE/RECONCILE",
            "交易所快照无对应持仓，按原规则逻辑核销",
            SSD=ssd_key,
            OpenRecordID=linked_open_id,
            Hint="可能已手动平仓/外部减仓/本轮快照尚未反映新成交",
        ))
        return

    if actual_position < ledger_open_amount * (
        1 - POSITION_DIFF_TOLERANCE
    ):
        logger.warning(_log(
            "CLOSE/WARN",
            "实际持仓不足覆盖账本开仓量，将按实际持仓封顶",
            SSD=ssd_key,
            Ledger=f"{ledger_open_amount:.6f}",
            Exchange=f"{actual_position:.6f}",
            Hint="检查手动减仓/其它策略/账实差异",
        ))

    amount = amount_to_precision(
        exchange,
        sig["symbol"],
        min(
            ledger_open_amount,
            actual_position,
        ),
    )

    if amount <= 0:
        logger.warning(_log(
            "CLOSE/SKIP",
            "精度处理后平仓数量为 0",
            SSD=ssd_key,
            LedgerAmount=ledger_open_amount,
            ActualPosition=actual_position,
        ))
        return

    # :
    # 原代码注释称“reduce_only + 持仓方向严格只减仓”，但实际参数一直为 False。
    # 双向持仓模式/网关实现可能对此有特殊要求，业务语义未确认前保留原参数。
    result = execute_order(
        exchange=exchange,
        symbol=sig["symbol"],
        side=sig["action"],
        amount=amount,
        client_oid=sig["client_oid"],
        order_type="market",
        reduce_only=False,
        position_side=sig["direction"],
    )
    status = ST_PENDING if result.ok else ST_FAILED

    ledger.append(make_record(
        sig,
        amount,
        amount * sig["price"],
        status,
        sig["client_oid"],
        result.exchange_oid,
        error_msg=result.error_msg,
        linked_open_id=linked_open_id,
    ))

    if result.ok:
        _cache_order(
            open_order_cache,
            sig["symbol"],
            result.exchange_oid,
            sig["client_oid"],
        )

    log_fn = logger.info if result.ok else logger.error
    log_fn(_log(
        "CLOSE/ORDER",
        "平仓请求已完成",
        SSD=ssd_key,
        Amount=amount,
        Status=status,
        OpenRecordID=linked_open_id,
        ClientOID=sig["client_oid"],
        **({"Reason": result.error_msg} if result.error_msg else {}),
    ))


def execute_single_signal(
    exchange,
    row,
    total_equity,
    position_cache,
    open_order_cache,
    ledger,
):
    """每个信号执行前读取最新账本，使同轮后续信号能看到前序账本写入。"""
    if position_cache is None or open_order_cache is None:
        logger.error(_log(
            "EXEC/SKIP",
            "账户缓存缺失，拒绝执行当前信号",
            Reason="持仓或挂单快照不可用",
        ))
        return

    sig = parse_signal(row)
    ledger_df = ledger.read()

    if sig["event"] == EVENT_OPEN:
        handle_open(
            exchange,
            ledger,
            ledger_df,
            sig,
            total_equity,
            open_order_cache,
        )
        return

    if sig["event"] == EVENT_CLOSE:
        handle_close(
            exchange,
            ledger,
            ledger_df,
            sig,
            position_cache,
            open_order_cache,
        )
        return

    logger.warning(_log(
        "EXEC/SKIP",
        "未知事件类型",
        Event=sig["event"],
        SSD=sig["ssd_key"],
    ))


def execute_signals(
    exchange,
    target_time,
    total_equity,
    position_cache,
    open_order_cache,
    signal_df,
    ledger,
):
    """
    执行 target_time ± SIGNAL_WINDOW_MIN 内的信号；
    单信号异常隔离。
    signal_df 至少含 time 与 parse_signal 所需字段。
    """
    started_at = time.perf_counter()
    lower = target_time - timedelta(
        minutes=SIGNAL_WINDOW_MIN
    )
    upper = target_time + timedelta(
        minutes=SIGNAL_WINDOW_MIN
    )

    times = pd.to_datetime(
        signal_df["time"],
        errors="coerce",
    )
    if getattr(times.dt, "tz", None) is not None:
        times = times.dt.tz_localize(None)

    valid = signal_df[
        (times >= lower)
        & (times <= upper)
    ]

    if valid.empty:
        logger.info(_log(
            "EXEC/SUMMARY",
            "本轮没有落入执行窗口的信号",
            Target=target_time.strftime(
                "%Y-%m-%d %H:%M:%S"
            ),
            Window=f"±{SIGNAL_WINDOW_MIN}min",
        ))
        return

    for _, row in valid.iterrows():
        try:
            execute_single_signal(
                exchange,
                row,
                total_equity,
                position_cache,
                open_order_cache,
                ledger,
            )
        except Exception as exc:
            logger.error(
                _log(
                    "EXEC/ERROR",
                    "单信号执行失败，已隔离并继续其它信号",
                    Reason=exc,
                    Hint="检查信号字段/精度/网关返回/账本状态",
                )
                + f"\n{traceback.format_exc()}"
            )

    logger.info(_log(
        "EXEC/SUMMARY",
        "本轮信号处理完成",
        ValidSignals=f"{len(valid)}笔",
        Elapsed=(
            f"{(time.perf_counter() - started_at) * 1000:.1f}ms"
        ),
    ))


# =============================================================================
# L5. 监控币种、信号生成与盘后汇总
# =============================================================================
def print_position_summary(
    exchange,
    ledger,
    open_order_cache=None,
):
    """盘后对账并重新拉取最新持仓，以单条聚合日志输出全部理论未平仓。"""
    reconcile_ledger(
        exchange,
        ledger,
        open_order_cache,
    )

    position_cache = _retry_fetch(
        "汇总持仓",
        lambda: fetch_positions_map(exchange),
    )
    if position_cache is None:
        logger.warning(_log(
            "SUMMARY/SKIP",
            "最新持仓拉取失败，无法输出汇总",
            Hint="检查账户接口/网络",
        ))
        return

    active_opens = _active_opens(
        ledger.read()
    )
    if active_opens.empty:
        logger.info(_log(
            "SUMMARY/POSITION",
            "本轮结束",
            TheoreticalOpen=0,
            Result="当前无理论持仓",
        ))
        return

    lines = []
    for _, row in active_opens.iterrows():
        symbol = str(
            row.get("symbol", "")
        ).strip()
        direction = str(
            row.get("direction", "")
        ).strip().upper()

        price = str(
            row.get("actual_fill_price", "")
        ).strip()
        price = (
            "N/A"
            if not price or price.lower() == "nan"
            else price
        )

        lines.append(
            f"  - Symbol:[{symbol}] "
            f"Direction:[{direction}] "
            f"OpenRecordID:[{str(row.get('record_id', ''))[:12]}] "
            f"FillPrice:[{price}] "
            f"LedgerAmount:[{to_num(row.get('filled_amount'))}] "
            f"ExchangeAmount:[{abs(position_cache.get(make_position_key(symbol, direction), 0.0))}]"
        )

    logger.info(
        _log(
            "SUMMARY/POSITION",
            "本轮结束",
            TheoreticalOpen=f"{len(lines)}笔",
        )
        + " | Details:\n"
        + "\n".join(lines)
    )


def get_top_movers(
    exchange,
    top_n=10,
    mode="top",
):
    """返回 USDT 合约涨跌幅榜；mode 支持 top/bottom/both。"""
    changes = pd.Series(
        fetch_usdt_swap_changes(exchange),
        dtype="float64",
    ).sort_values(
        ascending=False
    )

    if mode == "top":
        return changes.head(
            top_n
        ).index.tolist()

    if mode == "bottom":
        return changes.tail(
            top_n
        )[::-1].index.tolist()

    if mode == "both":
        return {
            "top": changes.head(
                top_n
            ).index.tolist(),
            "bottom": changes.tail(
                top_n
            )[::-1].index.tolist(),
        }

    raise ValueError(
        "mode 参数必须是 'top', 'bottom' 或 'both'"
    )


def build_monitor_symbols(
    exchange,
    position_cache,
    ledger,
    top_n,
    mode,
):
    """
    监控币种 = 榜单币种 ∪ (实际持仓币种 ∩ 本策略理论持仓币种)。
    position_cache Shape: {key: amount}。
    """
    actual_symbols = {
        position_key_symbol(key)
        for key in (position_cache or {}).keys()
    }

    theoretical_symbols = {
        str(row["symbol"]).strip()
        for _, row in _active_opens(
            ledger.read()
        ).iterrows()
    }

    holding_symbols = list(
        actual_symbols.intersection(
            theoretical_symbols
        )
    )
    ranked_symbols = get_top_movers(
        exchange,
        top_n=top_n,
        mode=mode,
    )

    # 保留原实现的 set 合并语义，不引入新的顺序假设。
    final_symbols = list(
        set(
            ranked_symbols
            + holding_symbols
        )
    )

    logger.info(_log(
        "SIGNAL/MONITOR",
        "本轮监控币种已生成",
        ExchangeHoldings=len(actual_symbols),
        LedgerHoldings=len(theoretical_symbols),
        StrategyIntersection=len(holding_symbols),
        FinalCount=len(final_symbols),
        Symbols=final_symbols,
    ))
    return final_symbols


def get_signal_df(
    exchange,
    target_time_str,
    proxy_url,
    position_cache,
    ledger,
):
    """
    按 CURRENT_SYMBOL 路由信号工作流。
    配置 Shape: (周期, 预加载提前量, workflow, rank_mode, top_n)。
    """
    config = STRATEGY_CONFIGS.get(
        CURRENT_SYMBOL
    )
    if config is None:
        logger.error(_log(
            "SIGNAL/CONFIG",
            "未知策略配置，无法生成信号",
            CURRENT_SYMBOL=CURRENT_SYMBOL,
        ))
        return None

    _, _, workflow, rank_mode, top_n = config

    if rank_mode is None:
        return workflow(
            target_time_str,
            proxy_url=proxy_url,
        )

    symbols = build_monitor_symbols(
        exchange,
        position_cache,
        ledger,
        top_n,
        rank_mode,
    )
    return workflow(
        target_time_str,
        symbol_list=symbols,
        proxy_url=proxy_url,
    )


# =============================================================================
# L6. 高可用调度器
# =============================================================================
def run_scheduler():
    """等待预加载时点 → 账户快照/对账 → 信号生成 → 窗口执行 → 盘后汇总。"""
    if platform.system().lower() == "linux":
        proxies, proxy_url = None, None
    else:
        proxies = {
            "http": "http://127.0.0.1:7890",
            "https": "http://127.0.0.1:7890",
        }
        proxy_url = "http://127.0.0.1:7890"

    exchange = open_session(
        proxies,
        account=ACCOUNT_ALIAS,
    )
    ledger = LedgerManager(
        LEDGER_FILE
    )

    logger.info(_log(
        "SCHED/START",
        "调度系统已启动",
        Strategy=CURRENT_SYMBOL,
        Account=ACCOUNT_ALIAS,
        Ledger=LEDGER_FILE,
    ))

    print_position_summary(
        exchange,
        ledger,
    )

    while True:
        try:
            now = datetime.now()
            config = STRATEGY_CONFIGS.get(
                CURRENT_SYMBOL
            )

            interval_minutes, preload_ahead = (
                (config[0], config[1])
                if config
                else (60, PRELOAD_AHEAD_MIN)
            )

            add_minutes = (
                interval_minutes
                - (now.minute % interval_minutes)
            )
            next_run = (
                now.replace(
                    second=0,
                    microsecond=0,
                )
                + timedelta(
                    minutes=add_minutes
                )
            )
            preload_time = (
                next_run
                - timedelta(
                    minutes=preload_ahead
                )
            )

            # :
            # 原逻辑的信号目标是 next_run - 1min，
            # 而执行窗口中心是 next_run。
            # 可能用于读取上一根闭合 K 线，也可能形成边界偏移；
            # 业务含义未确认，保持不变。
            target_time_str = (
                next_run
                - timedelta(minutes=1)
            ).strftime(
                "%Y-%m-%d %H:%M"
            )

            logger.info(_log(
                "SCHED/PLAN",
                "下一轮计划已计算",
                NextRun=next_run.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                PreloadAt=preload_time.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                Now=now.strftime(
                    "%Y-%m-%d %H:%M:%S"
                ),
                SignalTarget=target_time_str,
            ))

            if now < preload_time:
                time.sleep(
                    (
                        preload_time
                        - now
                    ).total_seconds()
                )

            equity = 0.0
            position_cache = None
            open_order_cache = None
            preload_ok = False

            for attempt in range(
                1,
                API_MAX_RETRY + 1,
            ):
                try:
                    (
                        equity,
                        position_cache,
                        open_order_cache,
                    ) = preload_account_state(
                        exchange,
                        ledger,
                    )

                    if (
                        equity > 0
                        and position_cache is not None
                        and open_order_cache is not None
                    ):
                        preload_ok = True
                        break

                except Exception as exc:
                    logger.error(
                        _log(
                            "PRELOAD/ROUND",
                            "账户预加载异常，将重试",
                            Attempt=f"{attempt}/{API_MAX_RETRY}",
                            Reason=exc,
                        )
                        + f"\n{traceback.format_exc()}"
                    )

                time.sleep(3)

            if not preload_ok:
                logger.error(_log(
                    "SCHED/SKIP",
                    "账户快照连续失败，放弃当前轮次",
                    Attempts=API_MAX_RETRY,
                    Next="60 秒后重新进入调度计算",
                ))
                time.sleep(60)
                continue

            signal_df = get_signal_df(
                exchange,
                target_time_str,
                proxy_url,
                position_cache,
                ledger,
            )

            if (
                signal_df is not None
                and not signal_df.empty
            ):
                execute_signals(
                    exchange,
                    next_run,
                    equity,
                    position_cache,
                    open_order_cache,
                    signal_df,
                    ledger,
                )
                logger.info(_log(
                    "SCHED/POST",
                    "信号执行阶段结束",
                    Next="盘后对账并刷新持仓汇总",
                ))
            else:
                logger.info(_log(
                    "SIGNAL/SUMMARY",
                    "本轮没有可执行信号",
                    Strategy=CURRENT_SYMBOL,
                    Target=target_time_str,
                ))

            print_position_summary(
                exchange,
                ledger,
                open_order_cache,
            )

        except Exception as exc:
            logger.error(
                _log(
                    "SCHED/RECOVER",
                    "主循环异常，按原高可用策略 30 秒后重新进入循环",
                    Reason=exc,
                    Hint="查看堆栈定位未被局部隔离的异常",
                )
                + f"\n{traceback.format_exc()}"
            )
            time.sleep(30)


if __name__ == "__main__":
    run_scheduler()