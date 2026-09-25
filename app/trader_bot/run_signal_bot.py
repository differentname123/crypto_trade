# -*- coding: utf-8 -*-
"""
[功能摘要] 跨周期信号交易执行系统：按策略原子单元隔离，通过 REST 快照、CSV 账本对账与时间窗信号执行完成开平仓闭环。
[多点并发支持版本] 面向对象重构：支持多账号、多策略以 Thread Worker 形式并行运行，通过独立账本与上下文隔离。
"""
import multiprocessing
import os
import platform
import threading
import time
import traceback
import uuid
import logging
from datetime import datetime, timedelta

import pandas as pd

from common_utils import setup_logger, get_config
from signal_generator import (
    execute_trading_bot_high_fr_bear_div_short, execute_trading_bot_oi_decay_short,
    execute_trading_bot_vwap_reclaim_long, execute_trading_bot_workflow_XSR_long,
    execute_trading_bot_workflow_bottom_powder_short, execute_trading_bot_workflow_cross,
    execute_trading_bot_workflow_ma_bottom_long, execute_trading_bot_workflow_short_fr,
    execute_trading_bot_workflow_top_long, execute_trading_bot_workflow_vol_fr_long,
)

# ===== 交易所平台选择 (仅需修改此处即可切换平台) =====
EXCHANGE_PLATFORM = "binance"   # 可选: "binance" | "okx" | "bybit"
# ====================================================

from exchange_factory import get_gateway
_gw = get_gateway(EXCHANGE_PLATFORM)

# 平台无关的常量 (走平台路由, 所有 gateway 均导出相同定义)
OS_CANCELED = _gw.OS_CANCELED
OS_FILLED = _gw.OS_FILLED
OS_OPEN = _gw.OS_OPEN
OS_REJECTED = _gw.OS_REJECTED

# 以下函数全部走平台路由
amount_to_precision = _gw.amount_to_precision
build_client_oid = _gw.build_client_oid
cancel_order_by_id = _gw.cancel_order_by_id
execute_order = _gw.execute_order
extract_order_view = _gw.extract_order_view
fetch_open_orders_grouped = _gw.fetch_open_orders_grouped
fetch_order_by_id = _gw.fetch_order_by_id
fetch_positions_map = _gw.fetch_positions_map
fetch_recent_orders_map = _gw.fetch_recent_orders_map
fetch_total_equity = _gw.fetch_total_equity
fetch_usdt_swap_changes = _gw.fetch_usdt_swap_changes
is_cancel_target_gone = _gw.is_cancel_target_gone
make_open_order_stub = _gw.make_open_order_stub
make_position_key = _gw.make_position_key
safe_init_exchange = _gw.safe_init_exchange
order_client_oid = _gw.order_client_oid
order_exchange_oid = _gw.order_exchange_oid
position_key_symbol = _gw.position_key_symbol
sync_exchange_time = _gw.sync_exchange_time

# =============================================================================
# L0. 常量定义与配置
# =============================================================================
BEST_TOP_N = 10
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DATA_DIR = os.path.join(BASE_DIR, "bot_data")

LEVERAGE = 1
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
# L1. 账本管理：文件锁覆盖本地读改写，增加全局字典锁防重入
# =============================================================================
class LedgerManager:
    COLUMNS = [
        "record_id", "signal_time", "strategy_name", "symbol", "direction", "event",
        "client_oid", "exchange_oid", "target_amount", "filled_amount", "actual_fill_price",
        "target_value", "exec_status", "linked_open_id", "update_time", "error_msg",
    ]

    _file_locks = {}
    _class_lock = threading.Lock()

    def __init__(self, file_path, log_func=None):
        dir_name = os.path.dirname(file_path)
        if dir_name:
            os.makedirs(dir_name, exist_ok=True)

        self.file_path = file_path
        self.tmp_path = file_path + ".tmp"
        self.log_func = log_func  # 注入的回调日志函数

        # 为每个物理文件分配独立的线程锁，防止不同线程误配置相同文件导致冲突
        with self._class_lock:
            if file_path not in self._file_locks:
                self._file_locks[file_path] = threading.Lock()
            self._lock = self._file_locks[file_path]

    def _read_unlocked(self):
        if not os.path.isfile(self.file_path):
            return pd.DataFrame(columns=self.COLUMNS)
        try:
            df = pd.read_csv(
                self.file_path,
                dtype={"record_id": str, "client_oid": str, "exchange_oid": str, "linked_open_id": str},
            )
        except Exception as exc:
            if self.log_func:
                self.log_func("error", "LEDGER/READ", "账本读取失败，按原设计使用空账本继续",
                              File=self.file_path, Reason=exc, Hint="检查 CSV 损坏/占用/权限")
            return pd.DataFrame(columns=self.COLUMNS)

        for column in self.COLUMNS:
            if column not in df.columns:
                df[column] = ""
        return df

    def _atomic_write_unlocked(self, df):
        df.to_csv(self.tmp_path, index=False, encoding="utf-8")
        os.replace(self.tmp_path, self.file_path)

    def read(self):
        with self._lock:
            return self._read_unlocked()

    def append(self, record):
        with self._lock:
            df = self._read_unlocked()
            row = {column: record.get(column, "") for column in self.COLUMNS}
            self._atomic_write_unlocked(pd.concat([df, pd.DataFrame([row])], ignore_index=True))

    def apply_updates(self, updates_map):
        if not updates_map:
            return
        with self._lock:
            df = self._read_unlocked()
            record_ids = df["record_id"].astype(str)
            for record_id, fields in updates_map.items():
                mask = record_ids == str(record_id)
                if not mask.any():
                    if self.log_func:
                        self.log_func("warning", "LEDGER/UPDATE", "跳过不存在的账本记录",
                                      RecordID=record_id, Hint="账本可能被人工修改或版本不一致")
                    continue
                for column, value in fields.items():
                    df.loc[mask, column] = value
            self._atomic_write_unlocked(df)


# =============================================================================
# L2. 领域纯函数工具 (无需注入上下文)
# =============================================================================
def to_num(value, default=0.0):
    number = pd.to_numeric(value, errors="coerce")
    return float(number) if pd.notna(number) else default


def _safe_to_datetime(value):
    try:
        return pd.to_datetime(value).to_pydatetime()
    except Exception:
        return None


def map_exchange_status(exchange_status):
    return _EX_TO_LEDGER_STATUS.get(str(exchange_status).upper(), ST_PENDING)


def parse_signal(row):
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
    if df.empty: return df
    return df[
        (df["strategy_name"].astype(str).str.strip() == sig["strategy_name"])
        & (df["symbol"].astype(str).str.strip() == sig["symbol"])
        & (df["direction"].astype(str).str.strip().str.upper() == sig["direction"])
        ]


def _closed_open_ids(df):
    if df.empty: return set()
    closes = df[
        (df["event"].astype(str).str.strip().str.upper() == EVENT_CLOSE)
        & (~df["exec_status"].astype(str).isin([ST_FAILED, ST_CANCELED]))
        ]
    ids = closes["linked_open_id"].astype(str).str.strip()
    return {value for value in ids if value and value.lower() != "nan"}


def _active_opens(df):
    if df.empty: return df
    opens = df[df["event"].astype(str).str.strip().str.upper() == EVENT_OPEN]
    if opens.empty: return opens
    filled = pd.to_numeric(opens["filled_amount"], errors="coerce").fillna(0)
    return opens[(~opens["record_id"].astype(str).isin(_closed_open_ids(df))) & (filled > 0)]


def _find_open_to_close(ssd_df):
    active = _active_opens(ssd_df)
    return None if active.empty else active.iloc[-1]


def _has_pending_order(open_order_cache, sig):
    return any(
        (order_client_oid(order) or "").startswith(sig["prefix"])
        for order in open_order_cache.get(sig["symbol"], [])
    )


def _cache_order(open_order_cache, symbol, exchange_oid, client_oid):
    open_order_cache.setdefault(symbol, []).append(make_open_order_stub(exchange_oid, client_oid))


# =============================================================================
# L3~L6. 并行工作单元：TradingWorker
# =============================================================================
class TradingWorker:
    """原子化交易单元，挂载其独立的 账户/网关/账本/状态。"""

    def __init__(self, account_alias, strategy_name):
        self.account_alias = account_alias
        self.strategy_name = strategy_name

        # 【核心修改】：加上 force_reset=True，在子进程启动时强制切断与父进程共用的文件句柄
        self.logger = setup_logger(
            app_name=f"{account_alias}_{strategy_name}_trader",
            force_reset=True
        )

        self._last_equity = 0.0

        # 分离账本文件
        self.ledger_file = os.path.join(DATA_DIR, f"trade_records_{account_alias}_{strategy_name}.csv")
        self.ledger = LedgerManager(self.ledger_file, log_func=self.log)

        # 动态限额
        self.min_order_value = 51 if strategy_name == "cross" else 6
        self.max_order_value = 2000.0 if strategy_name == "cross" else 500

        # 代理设置
        if platform.system().lower() == "linux":
            self.proxies, self.proxy_url = None, None
        else:
            self.proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
            self.proxy_url = "http://127.0.0.1:7890"

        # 凭证加载与交易所实例化走平台路由, 切换 EXCHANGE_PLATFORM 即自动匹配
        self.exchange = _gw.open_session(self.proxies, self.account_alias)

    def log(self, level, scope, message, **fields):
        """实例级日志门面，强制注入账户和策略上下文。"""
        fields = {"Account": self.account_alias, "Strategy": self.strategy_name, **fields}
        msg = _log(scope, message, **fields)
        getattr(self.logger, level)(msg)

    def _retry_fetch(self, label, fetch_fn):
        for attempt in range(1, API_MAX_RETRY + 1):
            try:
                return fetch_fn()
            except Exception as exc:
                self.log("warning", "PRELOAD/FETCH", f"{label}拉取失败，将重试",
                         Attempt=f"{attempt}/{API_MAX_RETRY}", Reason=exc, Hint="检查网络/接口/会话")
                time.sleep(1)
        self.log("error", "PRELOAD/FETCH", f"{label}连续拉取失败",
                 Attempts=API_MAX_RETRY, Result="本轮拒绝使用缺失快照交易")
        return None

    def _is_order_timeout(self, row, now):
        event = str(row.get("event", "")).strip().upper()
        timeout_hours = OPEN_ORDER_TIMEOUT_HOURS if event == EVENT_OPEN else CLOSE_ORDER_TIMEOUT_HOURS
        signal_time = _safe_to_datetime(row.get("signal_time"))
        if signal_time is None:
            return False, timeout_hours
        return (now - signal_time) > timedelta(hours=timeout_hours), timeout_hours

    def reconcile_ledger(self, open_order_cache):
        started_at = time.perf_counter()
        df = self.ledger.read()
        if df.empty: return
        pending = df[df["exec_status"].astype(str) == ST_PENDING]
        if pending.empty: return

        now = datetime.now()
        now_str = now.strftime("%Y-%m-%d %H:%M:%S")
        updates, synced_count, timeout_count = {}, 0, 0

        open_orders_by_id = {}
        for orders in (open_order_cache or {}).values():
            for order in orders:
                exchange_oid = order_exchange_oid(order)
                if exchange_oid: open_orders_by_id[exchange_oid] = order

        missing_symbols = set()
        for _, row in pending.iterrows():
            exchange_oid = str(row.get("exchange_oid", "")).strip()
            if exchange_oid and exchange_oid.lower() != "nan" and exchange_oid not in open_orders_by_id:
                symbol = str(row.get("symbol", "")).strip()
                if symbol: missing_symbols.add(symbol)

        recent_orders_by_id = {}
        for symbol in missing_symbols:
            try:
                recent_orders_by_id.update(fetch_recent_orders_map(self.exchange, symbol, limit=RECENT_ORDER_LIMIT))
            except Exception as exc:
                self.log("warning", "RECON/BATCH", "近期订单批量查询失败，将由单笔查询兜底", Symbol=symbol, Reason=exc)
                time.sleep(0.5)

        for _, row in pending.iterrows():
            record_id = str(row["record_id"])
            exchange_oid = str(row.get("exchange_oid", "")).strip()
            symbol = str(row.get("symbol", "")).strip()
            if not exchange_oid or exchange_oid.lower() == "nan": continue

            order_info = open_orders_by_id.get(exchange_oid, recent_orders_by_id.get(exchange_oid))
            if order_info is None:
                try:
                    time.sleep(0.1)
                    order_info = fetch_order_by_id(self.exchange, symbol, exchange_oid)
                except Exception as exc:
                    self.log("warning", "RECON/LOOKUP", "单笔订单查询失败，本轮暂不改账",
                             RecordID=record_id, ExchangeOID=exchange_oid, Symbol=symbol, Reason=exc)
                    continue

            if not order_info: continue

            view = extract_order_view(order_info)
            exchange_status = view["status"]
            filled_amount = to_num(view["filled"])
            avg_price = view["avg_price"]
            is_timeout, timeout_hours = self._is_order_timeout(row, now)

            if exchange_status == OS_OPEN and is_timeout:
                event = str(row.get("event", "")).strip().upper()
                final_status = OS_CANCELED
                try:
                    cancel_order_by_id(self.exchange, symbol, exchange_oid)
                    try:
                        final_view = extract_order_view(fetch_order_by_id(self.exchange, symbol, exchange_oid))
                        final_status = final_view["status"]
                        filled_amount = to_num(final_view["filled"], filled_amount)
                        avg_price = final_view["avg_price"] or avg_price
                    except Exception as exc:
                        self.log("warning", "RECON/CANCEL", "撤单已提交，但终态复查失败；按已撤单继续核销",
                                 ExchangeOID=exchange_oid, Reason=exc)

                    ledger_status = map_exchange_status(final_status)
                    if ledger_status == ST_PENDING: ledger_status = ST_CANCELED

                    update = {
                        "exec_status": ledger_status, "update_time": now_str,
                        "error_msg": f"{event}单超时(>{timeout_hours}H)自动撤单",
                    }
                    if filled_amount > 0:
                        update.update({"filled_amount": filled_amount, "actual_fill_price": avg_price})

                    updates[record_id] = update
                    timeout_count += 1
                    self.log("info", "RECON/CANCEL", "超时订单已处理",
                             Event=event, ExchangeOID=exchange_oid, FinalStatus=ledger_status, Filled=filled_amount)
                except Exception as exc:
                    if is_cancel_target_gone(exc):
                        updates[record_id] = {
                            "exec_status": ST_CANCELED, "update_time": now_str,
                            "error_msg": f"撤单查无此单, 强制核销: {exc}",
                        }
                        timeout_count += 1
                        self.log("warning", "RECON/CANCEL", "撤单目标已不存在，按原规则强制核销",
                                 ExchangeOID=exchange_oid, Result=ST_CANCELED,
                                 Hint="订单可能已被交易所清理或历史查询窗口已过")
                    else:
                        updates[record_id] = {"update_time": now_str, "error_msg": f"超时撤单失败: {exc}"}
                        self.log("warning", "RECON/CANCEL", "超时撤单失败，保留 PENDING 等待下轮",
                                 ExchangeOID=exchange_oid, Reason=exc, Hint="检查连接/订单状态/撤单权限")
                continue

            ledger_status = map_exchange_status(exchange_status)
            if ledger_status == ST_PENDING and filled_amount <= 0: continue

            update = {"update_time": now_str}
            if ledger_status != ST_PENDING: update["exec_status"] = ledger_status
            if filled_amount > 0: update.update({"filled_amount": filled_amount, "actual_fill_price": avg_price})

            updates[record_id] = update
            synced_count += 1

        self.ledger.apply_updates(updates)
        if synced_count or timeout_count:
            self.log("info", "RECON/SUMMARY", "PENDING 对账完成",
                     Synced=f"{synced_count}笔", TimeoutHandled=f"{timeout_count}笔",
                     Elapsed=f"{(time.perf_counter() - started_at) * 1000:.0f}ms")

    def check_position_consistency(self, position_cache):
        active_opens = _active_opens(self.ledger.read())
        if active_opens.empty: return

        expected_by_position = {}
        for _, row in active_opens.iterrows():
            pos_key = make_position_key(str(row["symbol"]).strip(), str(row["direction"]).strip())
            expected_by_position[pos_key] = expected_by_position.get(pos_key, 0.0) + to_num(row["filled_amount"])

        for pos_key, expected_amount in expected_by_position.items():
            actual_amount = abs(position_cache.get(pos_key, 0.0))
            if actual_amount < expected_amount * (1 - POSITION_DIFF_TOLERANCE):
                self.log("warning", "RECON/POSITION", "账本理论持仓高于交易所实际持仓，不自动调整",
                         Position=pos_key, Ledger=f"{expected_amount:.6f}", Exchange=f"{actual_amount:.6f}",
                         Tolerance=f"{POSITION_DIFF_TOLERANCE:.2%}", Hint="检查手动减仓/外部策略/成交回填")

    def preload_account_state(self):
        started_at = time.perf_counter()
        try:
            sync_exchange_time(self.exchange)
        except Exception as exc:
            self.log("warning", "PRELOAD/TIME", "交易所校时失败，按原设计继续本轮",
                     Reason=exc, Hint="若后续签名报时间偏差，检查本机时钟/网络延迟")

        equity = 0.0
        for attempt in range(1, API_MAX_RETRY + 1):
            try:
                equity = fetch_total_equity(self.exchange)
            except Exception as exc:
                equity = 0.0
                self.log("warning", "PRELOAD/EQUITY", "权益接口异常，将重试", Attempt=f"{attempt}/{API_MAX_RETRY}",
                         Reason=exc)

            if equity > 0:
                self._last_equity = equity
                break
            time.sleep(1)

        if equity <= 0:
            equity = self._last_equity
            self.log("warning", "PRELOAD/EQUITY", "未取得有效权益，沿用上轮缓存",
                     CachedEquity=f"{equity:.2f}", Impact="缓存也为 0 时本轮最终会被放弃")

        position_cache = self._retry_fetch("持仓", lambda: fetch_positions_map(self.exchange))
        open_order_cache = self._retry_fetch("挂单", lambda: fetch_open_orders_grouped(self.exchange))

        if open_order_cache is not None:
            self.reconcile_ledger(open_order_cache)
        if position_cache is not None:
            self.check_position_consistency(position_cache)

        pos_count = "N/A" if position_cache is None else len(position_cache)
        order_count = "N/A" if open_order_cache is None else sum(len(v) for v in open_order_cache.values())

        self.log("info", "PRELOAD/SUMMARY", "账户快照完成",
                 Equity=f"{equity:.2f}", Positions=pos_count, OpenOrders=order_count,
                 Elapsed=f"{(time.perf_counter() - started_at) * 1000:.0f}ms")
        return equity, position_cache, open_order_cache

    def handle_open(self, ledger_df, sig, total_equity, open_order_cache):
        ssd_key = sig["ssd_key"]
        ssd_df = _filter_ssd(ledger_df, sig)

        if _has_pending_order(open_order_cache, sig):
            self.log("warning", "OPEN/SKIP", "检测到同信号挂单，阻止重复发单", SSD=ssd_key, Prefix=sig["prefix"])
            return

        if not _active_opens(ssd_df).empty:
            self.log("warning", "OPEN/WARN", "本 SSD 仍有未平成交开仓，但按原规则继续执行新信号",
                     SSD=ssd_key, Hint="检查上一对开平信号或人工干预")

        target_value = min(max(total_equity * LEVERAGE * sig["max_weight"], self.min_order_value), self.max_order_value)
        amount = amount_to_precision(self.exchange, sig["symbol"], target_value / sig["price"])

        if amount <= 0:
            self.log("warning", "OPEN/SKIP", "精度处理后下单数量为 0",
                     SSD=ssd_key, TargetValue=f"{target_value:.2f}", Price=sig["price"])
            return

        result = execute_order(
            exchange=self.exchange, symbol=sig["symbol"], side=sig["action"],
            amount=amount, client_oid=sig["client_oid"], order_type="market",
            reduce_only=False, position_side=sig["direction"]
        )
        status = ST_PENDING if result.ok else ST_FAILED

        self.ledger.append(make_record(
            sig, amount, target_value, status, sig["client_oid"], result.exchange_oid, error_msg=result.error_msg,
        ))

        if result.ok:
            _cache_order(open_order_cache, sig["symbol"], result.exchange_oid, sig["client_oid"])

        level = "info" if result.ok else "error"
        self.log(level, "OPEN/ORDER", "开仓请求已完成",
                 SSD=ssd_key, Amount=amount, Value=f"{target_value:.2f}", Status=status, ClientOID=sig["client_oid"],
                 **({"Reason": result.error_msg} if result.error_msg else {}))

    def handle_close(self, ledger_df, sig, position_cache, open_order_cache):
        ssd_key = sig["ssd_key"]
        open_record = _find_open_to_close(_filter_ssd(ledger_df, sig))

        if open_record is None:
            self.log("info", "CLOSE/SKIP", "本 SSD 没有可关联的已成交开仓", SSD=ssd_key, Result="不发平仓单")
            return

        linked_open_id = str(open_record["record_id"])
        ledger_open_amount = to_num(open_record["filled_amount"])
        actual_position = abs(position_cache.get(sig["pos_key"], 0.0))

        if actual_position <= 0:
            self.ledger.append(make_record(
                sig, 0, 0, ST_MANUAL_CLOSED, sig["client_oid"], "",
                error_msg="平仓时交易所无持仓, 逻辑核销", linked_open_id=linked_open_id,
            ))
            self.log("warning", "CLOSE/RECONCILE", "交易所快照无对应持仓，按原规则逻辑核销",
                     SSD=ssd_key, OpenRecordID=linked_open_id, Hint="可能已手动平仓/外部减仓/本轮快照尚未反映新成交")
            return

        if actual_position < ledger_open_amount * (1 - POSITION_DIFF_TOLERANCE):
            self.log("warning", "CLOSE/WARN", "实际持仓不足覆盖账本开仓量，将按实际持仓封顶",
                     SSD=ssd_key, Ledger=f"{ledger_open_amount:.6f}", Exchange=f"{actual_position:.6f}",
                     Hint="检查手动减仓/其它策略/账实差异")

        amount = amount_to_precision(self.exchange, sig["symbol"], min(ledger_open_amount, actual_position))

        if amount <= 0:
            self.log("warning", "CLOSE/SKIP", "精度处理后平仓数量为 0",
                     SSD=ssd_key, LedgerAmount=ledger_open_amount, ActualPosition=actual_position)
            return

        result = execute_order(
            exchange=self.exchange, symbol=sig["symbol"], side=sig["action"],
            amount=amount, client_oid=sig["client_oid"], order_type="market",
            reduce_only=False, position_side=sig["direction"]
        )
        status = ST_PENDING if result.ok else ST_FAILED

        self.ledger.append(make_record(
            sig, amount, amount * sig["price"], status, sig["client_oid"],
            result.exchange_oid, error_msg=result.error_msg, linked_open_id=linked_open_id,
        ))

        if result.ok:
            _cache_order(open_order_cache, sig["symbol"], result.exchange_oid, sig["client_oid"])

        level = "info" if result.ok else "error"
        self.log(level, "CLOSE/ORDER", "平仓请求已完成",
                 SSD=ssd_key, Amount=amount, Status=status, OpenRecordID=linked_open_id, ClientOID=sig["client_oid"],
                 **({"Reason": result.error_msg} if result.error_msg else {}))

    def execute_single_signal(self, row, total_equity, position_cache, open_order_cache):
        if position_cache is None or open_order_cache is None:
            self.log("error", "EXEC/SKIP", "账户缓存缺失，拒绝执行当前信号", Reason="持仓或挂单快照不可用")
            return

        sig = parse_signal(row)
        ledger_df = self.ledger.read()

        if sig["event"] == EVENT_OPEN:
            self.handle_open(ledger_df, sig, total_equity, open_order_cache)
            return

        if sig["event"] == EVENT_CLOSE:
            self.handle_close(ledger_df, sig, position_cache, open_order_cache)
            return

        self.log("warning", "EXEC/SKIP", "未知事件类型", Event=sig["event"], SSD=sig["ssd_key"])

    def execute_signals(self, target_time, total_equity, position_cache, open_order_cache, signal_df):
        started_at = time.perf_counter()
        lower = target_time - timedelta(minutes=SIGNAL_WINDOW_MIN)
        upper = target_time + timedelta(minutes=SIGNAL_WINDOW_MIN)

        times = pd.to_datetime(signal_df["time"], errors="coerce")
        if getattr(times.dt, "tz", None) is not None:
            times = times.dt.tz_localize(None)

        valid = signal_df[(times >= lower) & (times <= upper)]

        if valid.empty:
            self.log("info", "EXEC/SUMMARY", "本轮没有落入执行窗口的信号",
                     Target=target_time.strftime("%Y-%m-%d %H:%M:%S"), Window=f"±{SIGNAL_WINDOW_MIN}min")
            return

        for _, row in valid.iterrows():
            try:
                self.execute_single_signal(row, total_equity, position_cache, open_order_cache)
            except Exception as exc:
                self.log("error", "EXEC/ERROR", "单信号执行失败，已隔离并继续其它信号",
                         Reason=exc, Hint="检查信号字段/精度/网关返回/账本状态")
                self.logger.error(f"\n{traceback.format_exc()}")

        self.log("info", "EXEC/SUMMARY", "本轮信号处理完成",
                 ValidSignals=f"{len(valid)}笔", Elapsed=f"{(time.perf_counter() - started_at) * 1000:.1f}ms")

    def print_position_summary(self, open_order_cache=None):
        self.reconcile_ledger(open_order_cache)
        position_cache = self._retry_fetch("汇总持仓", lambda: fetch_positions_map(self.exchange))

        # ================== 拉取或使用缓存的账户总权益 ==================
        current_equity = self._retry_fetch("汇总权益", lambda: fetch_total_equity(self.exchange))
        if current_equity is not None and current_equity > 0:
            self._last_equity = current_equity
        else:
            current_equity = self._last_equity
        # ====================================================================

        if position_cache is None:
            self.log("warning", "SUMMARY/SKIP", "最新持仓拉取失败，无法输出汇总", Hint="检查账户接口/网络")
            return

        active_opens = _active_opens(self.ledger.read())
        if active_opens.empty:
            # 顺手将权益记录到标准日志中，方便机器查阅
            self.log("info", "SUMMARY/POSITION", "本轮结束", TheoreticalOpen=0, Result="当前无理论持仓",
                     Equity=f"{current_equity:.2f}")
            return

        # ================== 新增：获取活动仓位标的的最新价格 ==================
        symbols_to_fetch = list(set([str(row.get("symbol", "")).strip() for _, row in active_opens.iterrows() if
                                     str(row.get("symbol", "")).strip()]))
        latest_prices = {}
        if symbols_to_fetch:
            try:
                # 批量获取持仓标的的 ticker 数据，以获取最新价 (CCXT 基础能力)
                tickers = self.exchange.fetch_tickers(symbols_to_fetch)
                for sym, ticker in tickers.items():
                    latest_prices[sym] = ticker.get('last') or ticker.get('close')
            except Exception as e:
                self.log("warning", "SUMMARY/PRICE", "获取最新价格失败，部分收益数据将不可用", Reason=e)
        # ====================================================================

        lines = []
        for _, row in active_opens.iterrows():
            symbol = str(row.get("symbol", "")).strip()
            direction = str(row.get("direction", "")).strip().upper()
            price_str = str(row.get("actual_fill_price", "")).strip()

            # 解析开仓均价
            try:
                open_price = float(price_str)
            except ValueError:
                open_price = 0.0

            ledger_amt = to_num(row.get('filled_amount'))
            exchange_amt = abs(position_cache.get(make_position_key(symbol, direction), 0.0))

            # ================== 新增：最新价格与理论收益计算 ==================
            latest_price = latest_prices.get(symbol)
            pnl_ratio_str = "N/A"
            pnl_value_str = "N/A"
            latest_price_str = "N/A"
            price_display = f"{open_price:g}" if open_price else "N/A"

            if latest_price and open_price > 0 and ledger_amt > 0:
                latest_price_str = f"{latest_price:g}"

                # 计算理论收益和涨跌幅 (按 USDT 本位线性合约计算公式)
                if direction == "LONG":
                    pnl_ratio = (latest_price - open_price) / open_price
                    pnl_value = (latest_price - open_price) * ledger_amt
                elif direction == "SHORT":
                    pnl_ratio = (open_price - latest_price) / open_price
                    pnl_value = (open_price - latest_price) * ledger_amt
                else:
                    pnl_ratio = 0.0
                    pnl_value = 0.0

                # 格式化: 强制带正负号，涨跌幅保留两位小数百分比，收益保留两位小数
                pnl_ratio_str = f"{pnl_ratio:+.2%}"
                pnl_value_str = f"{pnl_value:+.2f}"
            # ====================================================================

            # 使用方向图标和对齐排版增强视觉辨识度
            icon = "📈 [多]" if direction == "LONG" else "📉 [空]" if direction == "SHORT" else "⚪ [无]"

            # 扩展 f-string，加入开仓、现价、涨跌幅及收益
            line = (f" │ {icon} 标的: {symbol:<14} 开仓: {price_display:<9} 现价: {latest_price_str:<9} "
                    f"涨跌: {pnl_ratio_str:<9} 收益: {pnl_value_str:<8} "
                    f"账本: {ledger_amt:<7} 实际: {exchange_amt:<7} ID: {str(row.get('record_id', ''))[:8]:<8} │")
            lines.append(line)

        self.log("info", "SUMMARY/POSITION", "本轮结束", TheoreticalOpen=f"{len(lines)}笔",
                 Equity=f"{current_equity:.2f}")

        # 增加高对比度边框，因新增内容调整边框长度为 130（精算中英文字符终端全半角占位比）
        box_width = 130
        border_top = " ┍" + "━" * box_width + "┑"
        border_mid = " ┝" + "━" * box_width + "┥"
        border_bot = " ┕" + "━" * box_width + "┙"

        title = f" 💰 账户: [ {self.account_alias} ] | 权益: [ {current_equity:.2f} ] | 策略: [ {self.strategy_name} ] | 当前持仓明细 "

        display_block = (
                f"\n{border_top}\n"
                f" │ {title.center(box_width - 2, ' ')} │\n"
                f"{border_mid}\n" +
                "\n".join(lines) +
                f"\n{border_bot}"
        )

        self.logger.info(display_block)


    def get_top_movers(self, top_n=10, mode="top"):
        changes = pd.Series(fetch_usdt_swap_changes(self.exchange), dtype="float64").sort_values(ascending=False)
        if mode == "top": return changes.head(top_n).index.tolist()
        if mode == "bottom": return changes.tail(top_n)[::-1].index.tolist()
        if mode == "both":
            return {"top": changes.head(top_n).index.tolist(), "bottom": changes.tail(top_n)[::-1].index.tolist()}
        raise ValueError("mode 参数必须是 'top', 'bottom' 或 'both'")

    def build_monitor_symbols(self, position_cache, top_n, mode):
        actual_symbols = {position_key_symbol(key) for key in (position_cache or {}).keys()}
        theoretical_symbols = {str(row["symbol"]).strip() for _, row in _active_opens(self.ledger.read()).iterrows()}
        holding_symbols = list(actual_symbols.intersection(theoretical_symbols))
        ranked_symbols = self.get_top_movers(top_n=top_n, mode=mode)
        final_symbols = list(set(ranked_symbols + holding_symbols))

        self.log("info", "SIGNAL/MONITOR", "本轮监控币种已生成",
                 ExchangeHoldings=len(actual_symbols), LedgerHoldings=len(theoretical_symbols),
                 StrategyIntersection=len(holding_symbols), FinalCount=len(final_symbols), Symbols=final_symbols)
        return final_symbols

    def get_signal_df(self, target_time_str, position_cache):
        config = STRATEGY_CONFIGS.get(self.strategy_name)
        if config is None:
            self.log("error", "SIGNAL/CONFIG", "未知策略配置，无法生成信号")
            return None

        _, _, workflow, rank_mode, top_n = config
        if rank_mode is None:
            return workflow(target_time_str, proxy_url=self.proxy_url)

        symbols = self.build_monitor_symbols(position_cache, top_n, rank_mode)
        return workflow(target_time_str, symbol_list=symbols, proxy_url=self.proxy_url)

    def run(self):
        """Worker 调度主循环，线程专属阻塞周期"""
        self.log("info", "SCHED/START", "独立调度工作单元已启动", Ledger=self.ledger_file)
        self.print_position_summary()

        while True:
            try:
                now = datetime.now()
                config = STRATEGY_CONFIGS.get(self.strategy_name)
                interval_minutes, preload_ahead = (config[0], config[1]) if config else (60, PRELOAD_AHEAD_MIN)

                add_minutes = interval_minutes - (now.minute % interval_minutes)
                next_run = now.replace(second=0, microsecond=0) + timedelta(minutes=add_minutes)
                preload_time = next_run - timedelta(minutes=preload_ahead)
                target_time_str = (next_run - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")

                self.log("info", "SCHED/PLAN", "下一轮计划已计算",
                         NextRun=next_run.strftime("%Y-%m-%d %H:%M:%S"),
                         PreloadAt=preload_time.strftime("%Y-%m-%d %H:%M:%S"),
                         Now=now.strftime("%Y-%m-%d %H:%M:%S"), SignalTarget=target_time_str)

                if now < preload_time:
                    time.sleep((preload_time - now).total_seconds())

                equity = 0.0
                position_cache = None
                open_order_cache = None
                preload_ok = False

                for attempt in range(1, API_MAX_RETRY + 1):
                    try:
                        equity, position_cache, open_order_cache = self.preload_account_state()
                        if equity > 0 and position_cache is not None and open_order_cache is not None:
                            preload_ok = True
                            break
                    except Exception as exc:
                        self.log("error", "PRELOAD/ROUND", "账户预加载异常，将重试",
                                 Attempt=f"{attempt}/{API_MAX_RETRY}", Reason=exc)
                        self.logger.error(f"\n{traceback.format_exc()}")
                    time.sleep(3)

                if not preload_ok:
                    self.log("error", "SCHED/SKIP", "账户快照连续失败，放弃当前轮次", Attempts=API_MAX_RETRY,
                             Next="60 秒后重新进入调度计算")
                    time.sleep(60)
                    continue

                signal_df = self.get_signal_df(target_time_str, position_cache)

                if signal_df is not None and not signal_df.empty:
                    self.execute_signals(next_run, equity, position_cache, open_order_cache, signal_df)
                    # self.log("info", "SCHED/POST", "信号执行阶段结束", Next="盘后对账并刷新持仓汇总")
                else:
                    self.log("info", "SIGNAL/SUMMARY", "本轮没有可执行信号", Target=target_time_str)

                self.print_position_summary(open_order_cache)

            except Exception as exc:
                self.log("error", "SCHED/RECOVER", "主循环异常，按原高可用策略 30 秒后重新进入循环",
                         Reason=exc, Hint="查看堆栈定位未被局部隔离的异常")
                self.logger.error(f"\n{traceback.format_exc()}")
                time.sleep(30)


# =============================================================================
# L7. 并行任务配置与启动入口 (多进程模式重构)
# =============================================================================

# 请在此处配置你所需的账户与策略绑定关系，系统会自动并行调度
# 凭证由 gateway 的 open_session 按 EXCHANGE_PLATFORM 自动从配置文件读取，无需硬编码
WORKER_CONFIGS = [
    {"account": "mama",   "strategy": "cross"},
    {"account": "myself", "strategy": "cross"},
    {"account": "nana",   "strategy": "cross"},
    {"account": "qiqi",   "strategy": "cross"},
    {"account": "ruru",   "strategy": "cross"},
]


# 【新增核心函数】：模块顶层的独立运行空间，作为多进程的 target 入口
def _run_worker_process(cfg):
    """
    运行在独立的子进程内存中。此时实例化的 Worker 会独占网络会话、日志句柄和资源。
    """
    worker = TradingWorker(
        account_alias=cfg["account"],
        strategy_name=cfg["strategy"]
    )
    worker.run()


def main():
    print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 启动多账号、多策略并行调度中心(多进程模式)...")

    processes = []
    # 动态拉起物理隔离的工作进程
    for cfg in WORKER_CONFIGS:
        p = multiprocessing.Process(
            target=_run_worker_process,
            args=(cfg,),
            name=f"WorkerProcess-{cfg['account']}-{cfg['strategy']}"
        )
        p.daemon = True  # 设为 daemon，使子进程随主进程退出而安全终止
        p.start()
        processes.append(p)
        print(f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] 成功拉起工作进程: {p.name} (PID: {p.pid})")

    # 驻留主进程并守护子进程
    try:
        # 使用 join 阻塞主进程，保持守护状态
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        print("\n收到退出信号，系统安全终止，正在停止所有子进程...")


if __name__ == "__main__":
    main()
