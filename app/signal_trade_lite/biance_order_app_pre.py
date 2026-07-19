# -*- coding: utf-8 -*-
"""
跨周期信号交易执行系统 (纯 REST API)

设计原则: 简单为底 / 账本为核 / 隔离为纲 / 实事求是

顶层流程 (每整点驱动一轮 —— run_scheduler):
  ① 睡到整点前 PRELOAD_AHEAD_MIN 分钟
  ② preload_account_state : 拉权益/持仓/挂单 → reconcile_ledger 对账 → check_position_consistency 告警
  ③ execute_trading_bot_workflow : 拉取本轮信号
  ④ execute_signals       : 只执行 ±SIGNAL_WINDOW_MIN 分钟窗口内的信号 (OPEN→handle_open / CLOSE→handle_close)

隔离为纲: 最小原子单元 = 同策略的一对(开+平)信号, 以 ssd_key = strategy_symbol_direction 标识。
  各单元只读写自己的账本切片(_filter_ssd), 历史异常仅刺眼告警(⚠️)不阻断本单元或其它单元;
  是否开/平由 "当前信号 + 幂等 + 交易所实况" 决定, 不被历史账本一票否决。

实事求是: 平仓量 = min(本单元开仓实际成交量, 交易所该方向实际持仓); 只认实际成交与实际持仓, 不足则告警封顶。
"""
import os
import time
import uuid
import threading
import traceback
import platform
from datetime import datetime, timedelta

import pandas as pd

from common_utils_lite import get_config, setup_logger
logger = setup_logger(app_name="cross_momentum")


from run_cross_signal_lite import execute_trading_bot_workflow
from biance_order_lite import ( execute_order, get_total_equity,
    ExecStatus, safe_init_exchange
)

# ==========================================
# L0. 配置与常量
# ==========================================
LEDGER_FILE = "trade_records_debug.csv"       # 本策略专属账本, 与其它策略物理隔离
LEVERAGE = 1
MIN_ORDER_VALUE = 51
MAX_ORDER_VALUE = 2000.0

PRELOAD_AHEAD_MIN = 3                    # 整点前 N 分钟预对账
SIGNAL_WINDOW_MIN = 1                    # 信号有效窗口 (±N 分钟), 过期信号直接丢弃
OPEN_ORDER_TIMEOUT_HOURS = 2            # 开仓单超时清理阈值
CLOSE_ORDER_TIMEOUT_HOURS = 4          # 平仓单超时清理阈值
POSITION_DIFF_TOLERANCE = 0.01         # 持仓一致性告警容差 (1%)
API_MAX_RETRY = 3                       # 核心接口重试次数

# 账本内部订单状态枚举
ST_PENDING = "PENDING"                  # 已发出, 未完全确认
ST_FILLED = "FILLED"                    # 正常成交 (由 REST 回填数量与均价)
ST_CANCELED = "CANCELED"                # 主动或超时撤单
ST_FAILED = "FAILED"                    # 执行或网络彻底失败
ST_MANUAL_CLOSED = "MANUAL_CLOSED_NO_POSITION"  # 平仓时已无持仓, 逻辑核销

# 权益做软兜底(抖动不丢轮); 持仓/挂单做硬校验(拉取失败宁可放弃本轮, 拒绝脏数据交易)
_last_equity = 0.0


# ==========================================
# L1. 账本管理器: 所有读写强制经此 (细粒度锁 + 原子写, 网络操作全程不持锁)
# ==========================================
class LedgerManager:
    COLUMNS = [
        "record_id", "signal_time", "strategy_name", "symbol", "direction", "event",
        "client_oid", "exchange_oid",
        "target_amount", "filled_amount", "actual_fill_price", "target_value",
        "exec_status", "linked_open_id", "update_time", "error_msg"
    ]

    def __init__(self, file_path):
        self.file_path = file_path
        self.tmp_path = file_path + ".tmp"
        self._lock = threading.Lock()  # 仅在读改写文件的极短临界区加锁

    def _read_unlocked(self):
        """读账本; 文件缺失或损坏时返回带完整列头的空表, 保证下游列访问始终安全"""
        if not os.path.isfile(self.file_path):
            return pd.DataFrame(columns=self.COLUMNS)
        try:
            df = pd.read_csv(
                self.file_path,
                dtype={"record_id": str, "client_oid": str,
                       "exchange_oid": str, "linked_open_id": str}
            )
        except Exception as e:
            logger.error(f"[LEDGER] 读取失败: {e}")
            return pd.DataFrame(columns=self.COLUMNS)
        for col in self.COLUMNS:
            if col not in df.columns:
                df[col] = ""
        return df

    def _atomic_write_unlocked(self, df):
        """先写临时文件再 os.replace 原子替换; 即便替换瞬间断电, 原账本也毫发无损"""
        df.to_csv(self.tmp_path, index=False, encoding="utf-8")
        os.replace(self.tmp_path, self.file_path)

    def read(self):
        with self._lock:
            return self._read_unlocked()

    def append(self, record):
        """单笔落盘: 追加一条新记录 (原子写)"""
        with self._lock:
            df = self._read_unlocked()
            row = {col: record.get(col, "") for col in self.COLUMNS}
            df = pd.concat([df, pd.DataFrame([row])], ignore_index=True)
            self._atomic_write_unlocked(df)

    def apply_updates(self, updates_map):
        """按 record_id 批量更新字段并一次原子写. updates_map = {record_id: {field: value}}"""
        if not updates_map:
            return
        with self._lock:
            df = self._read_unlocked()
            ids = df["record_id"].astype(str)
            for rid, fields in updates_map.items():
                mask = ids == str(rid)
                if not mask.any():
                    logger.warning(f"[LEDGER] 未找到待更新记录: {rid}")
                    continue
                for k, v in fields.items():
                    df.loc[mask, k] = v
            self._atomic_write_unlocked(df)


# ==========================================
# L2. 领域工具 (纯函数: 换算 / 解析 / 映射)
# ==========================================
def to_num(val, default=0.0):
    """安全数值化: 无法解析时返回默认值"""
    n = pd.to_numeric(val, errors="coerce")
    return float(n) if pd.notna(n) else default


def _safe_to_datetime(val):
    """安全时间解析: 失败返回 None"""
    try:
        return pd.to_datetime(val).to_pydatetime()
    except Exception:
        return None


def map_exchange_status(ccxt_status):
    """ccxt 订单状态 → 账本内部状态 (open/其它 → PENDING 继续观察)"""
    s = str(ccxt_status).lower()
    if s == "closed":
        return ST_FILLED
    if s in ("canceled", "expired"):
        return ST_CANCELED
    if s == "rejected":
        return ST_FAILED
    return ST_PENDING


def parse_signal(row):
    """信号行 → 标准化字典, 并派生隔离 Key(ssd_key) 与可溯源 Client OID"""
    st = pd.to_datetime(row["time"])
    # API 时间若带时区, 剥离为 naive, 与本地 naive 时间安全对比 (否则窗口过滤会报错)
    if getattr(st, "tzinfo", None) is not None:
        st = st.tz_localize(None)

    strategy = str(row.get("STRATEGY_NAME", "DEF")).strip()
    symbol = str(row["symbol"]).strip()
    coin = str(row["coin"]).strip().upper()
    direction = str(row["direction"]).strip().upper()
    event = str(row["event"]).strip().upper()
    action = str(row["action"]).strip().lower()
    # Client OID 前缀: 策略_币种_方向_开平_时间戳, 既全局唯一又能一眼识别归属
    prefix = f"{strategy[-6:]}_{coin[:4]}_{direction[:1]}_{event[:1]}_{st.strftime('%d%H%M')}"
    return {
        "signal_time": st,
        "strategy_name": strategy,
        "symbol": symbol,
        "coin": coin,
        "direction": direction,
        "event": event,
        "action": action,
        "price": float(row["price"]),
        "max_weight": float(row.get("max_weight", 0.1)),
        "ssd_key": f"{strategy}_{symbol}_{direction}",  # 最小隔离单元
        "pos_key": f"{symbol}_{direction}",
        "prefix": prefix,
        "client_oid": f"{prefix}_{uuid.uuid4().hex[:5]}",
    }


def make_record(sig, target_amount, target_value, status, client_oid,
                exchange_oid, error_msg="", filled_amount="", actual_fill_price="",
                linked_open_id=""):
    """构造一条标准账本记录 (含全局唯一 record_id 与落盘时间)"""
    now_str = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    return {
        "record_id": uuid.uuid4().hex,
        "signal_time": sig["signal_time"].strftime("%Y-%m-%d %H:%M:%S"),
        "strategy_name": sig["strategy_name"],
        "symbol": sig["symbol"],
        "direction": sig["direction"],
        "event": sig["event"],
        "client_oid": client_oid,
        "exchange_oid": exchange_oid or "",
        "target_amount": target_amount,
        "filled_amount": filled_amount,
        "actual_fill_price": actual_fill_price,
        "target_value": target_value,
        "exec_status": status,
        "linked_open_id": linked_open_id,
        "update_time": now_str,
        "error_msg": error_msg,
    }


# ==========================================
# L3. 账本查询 (原子单元隔离视图: 涉及"历史"的查询均可限定在单个 SSD 单元内)
# ==========================================
def _filter_ssd(df, sig):
    """截取"本原子单元"(strategy + symbol + direction) 的账本切片, 与其它单元彻底隔离"""
    if df.empty:
        return df
    return df[
        (df["strategy_name"].astype(str).str.strip() == sig["strategy_name"]) &
        (df["symbol"].astype(str).str.strip() == sig["symbol"]) &
        (df["direction"].astype(str).str.strip().str.upper() == sig["direction"])
    ]


def _closed_open_ids(df):
    """
    已被"有效平仓"关联的开仓 record_id 集合。
    排除 FAILED/CANCELED 的平仓: 平仓失败或超时撤销时让其关联的开仓重获自由, 防死仓。
    """
    if df.empty:
        return set()
    closes = df[(df["event"].astype(str).str.strip().str.upper() == "CLOSE") &
                (~df["exec_status"].astype(str).isin([ST_FAILED, ST_CANCELED]))]
    return set(closes["linked_open_id"].astype(str))


def _find_open_to_close(ssd_df):
    """
    本单元内倒序寻找最近一条"有实际成交量且尚未平仓"的开仓。
    只认 filled_amount>0, 不纠结 PENDING/FILLED/CANCELED 状态 —— 超时撤单产生的部分成交残仓也须能被如实平掉。
    """
    if ssd_df.empty:
        return None
    closed = _closed_open_ids(ssd_df)
    opens = ssd_df[ssd_df["event"].astype(str).str.strip().str.upper() == "OPEN"]
    if opens.empty:
        return None
    for i in range(len(opens) - 1, -1, -1):
        rec = opens.iloc[i]
        if str(rec["record_id"]) in closed:
            continue
        if to_num(rec["filled_amount"]) > 0:
            return rec
    return None


def _has_unclosed_open(ssd_df):
    """本单元是否仍存在"有成交且未平仓"的开仓 (仅供开仓时刺眼告警, 不做拦截)"""
    return _find_open_to_close(ssd_df) is not None


def _has_pending_order(open_order_cache, sig):
    """幂等校验: 内存挂单中已有同前缀订单(=同一条信号)则视为重复, 防重复发单"""
    for o in open_order_cache.get(sig["symbol"], []):
        oid = o.get("clientOrderId") or o.get("info", {}).get("clientOrderId", "")
        if str(oid).startswith(sig["prefix"]):
            return True
    return False


def _cache_order(open_order_cache, symbol, exchange_oid, client_oid):
    """内存挂单打标, 封锁同批次重复开平 (下单成功后立即登记)"""
    open_order_cache.setdefault(symbol, []).append({
        "id": exchange_oid,
        "clientOrderId": client_oid,
        "info": {"clientOrderId": client_oid},
    })


# ==========================================
# L4. 预加载与对账 (执行前保证最终一致)
# ==========================================
def _retry_fetch(label, fn):
    """账户数据拉取带重试: 成功返回结果; 连续失败返回 None 并告警, 上层据此放弃本轮 (拒绝脏数据交易)"""
    for attempt in range(API_MAX_RETRY):
        try:
            return fn()
        except Exception as e:
            logger.warning(f"[PRELOAD] {label}拉取失败 {attempt + 1}/{API_MAX_RETRY} | {e}")
            time.sleep(1)
    logger.error(f"[PRELOAD] {label}连续拉取失败, 本轮将放弃")
    return None


def _fetch_positions(exchange):
    """拉取非零持仓, 归一化为 {symbol_SIDE: 带符号数量}; 单向持仓按数量正负推断多空"""
    cache = {}
    for pos in exchange.fetch_positions():
        amt = float(pos["info"]["positionAmt"])
        if amt == 0:
            continue
        side = str(pos["info"].get("positionSide", "")).upper()
        if not side or side == "BOTH":
            side = "LONG" if amt > 0 else "SHORT"
        cache[f"{pos['symbol']}_{side}"] = amt
    return cache


def _fetch_open_orders(exchange):
    """拉取全部活跃挂单, 按 symbol 分组缓存"""
    exchange.options["warnOnFetchOpenOrdersWithoutSymbol"] = False
    cache = {}
    for order in exchange.fetch_open_orders():
        cache.setdefault(order["symbol"], []).append(order)
    return cache


def _is_order_timeout(row, now):
    """
    按开/平类型分别判定挂单是否超时 (开 2h / 平 4h)。
    返回 (是否超时, 本类型阈值小时数); 时间不可解析时视为未超时。
    """
    event = str(row.get("event", "")).strip().upper()
    timeout_hours = OPEN_ORDER_TIMEOUT_HOURS if event == "OPEN" else CLOSE_ORDER_TIMEOUT_HOURS
    sig_time = _safe_to_datetime(row.get("signal_time"))
    if sig_time is None:
        return False, timeout_hours
    return (now - sig_time) > timedelta(hours=timeout_hours), timeout_hours


def reconcile_ledger(exchange, ledger, open_order_cache):
    """
    三级穿透对账 (只同步 PENDING 挂单, 从不臆测):
      L1 命中活跃挂单缓存(0 请求) → L2 按币种批量拉近期订单(每币种 1 请求) → L3 单笔兜底(每单 1 请求)。
    交易所仍 open 且已超时者按类型撤单; 撤单遇"查无此单"直接强制核销, 打破 PENDING 死循环。
    """
    t0 = time.perf_counter()
    df = ledger.read()
    if df.empty:
        return
    pending = df[df["exec_status"].astype(str) == ST_PENDING]
    if pending.empty:
        return

    now = datetime.now()
    now_str = now.strftime("%Y-%m-%d %H:%M:%S")
    updates = {}
    synced = canceled = 0

    # L1: 展平活跃挂单缓存 (exchange_oid → order)
    open_dict = {}
    for orders in (open_order_cache or {}).values():
        for o in orders:
            oid = str(o.get("id", ""))
            if oid:
                open_dict[oid] = o

    # L2: 对不在活跃挂单中的订单, 按币种批量拉取近期订单(含已成交/已撤销)
    missing_by_symbol = {}
    for _, row in pending.iterrows():
        oid = str(row.get("exchange_oid", "")).strip()
        if not oid or oid.lower() == "nan" or oid in open_dict:
            continue
        missing_by_symbol.setdefault(str(row.get("symbol", "")).strip(), []).append(oid)

    recent_dict = {}
    for sym in missing_by_symbol:
        try:
            for o in exchange.fetch_orders(sym, limit=50):
                recent_dict[str(o.get("id"))] = o
        except Exception as e:
            logger.warning(f"[RECON] 批量拉单失败 | {sym} | {e}")
            time.sleep(0.5)

    # 逐笔比对: 定位订单实况 → 超时撤单 → 状态/成交回填
    for _, row in pending.iterrows():
        rid = str(row["record_id"])
        oid = str(row.get("exchange_oid", "")).strip()
        symbol = str(row.get("symbol", "")).strip()
        if not oid or oid.lower() == "nan":
            continue

        # 三级穿透定位: 活跃缓存 → 近期批量 → 单笔兜底
        order_info = open_dict.get(oid, recent_dict.get(oid))
        if order_info is None:
            try:
                time.sleep(0.1)  # 防御性限流
                order_info = exchange.fetch_order(oid, symbol)
            except Exception as e:
                logger.warning(f"[RECON] 单笔兜底查询失败 | RID:{rid} OID:{oid} | {e}")
                continue
        if not order_info:
            continue

        ccxt_status = str(order_info.get("status", "")).lower()
        filled = to_num(order_info.get("filled"))
        avg = order_info.get("average") or order_info.get("price") or ""
        is_timeout, timeout_hours = _is_order_timeout(row, now)

        # 交易所仍挂单(open)且已超时 → 撤单
        if ccxt_status == "open" and is_timeout:
            event = str(row.get("event", "")).strip().upper()
            try:
                exchange.cancel_order(oid, symbol)
                try:
                    # 撤单后重拉终态, 防"撤单瞬间恰好成交"漏记成交量
                    fin = exchange.fetch_order(oid, symbol)
                    filled = to_num(fin.get("filled"), filled)
                    avg = fin.get("average") or fin.get("price") or avg
                except Exception:
                    pass
                upd = {"exec_status": ST_CANCELED, "update_time": now_str,
                       "error_msg": f"{event}单超时(>{timeout_hours}H)自动撤单"}
                if filled > 0:
                    upd["filled_amount"] = filled
                    upd["actual_fill_price"] = avg
                updates[rid] = upd
                canceled += 1
                logger.info(f"[RECON] 超时撤单 | {event} | OID:{oid} | 已成交残量:{filled}")
            except Exception as e:
                err = (type(e).__name__ + str(e)).lower()
                # "查无此单": 订单已被交易所清理, 直接核销, 打破 PENDING 死循环
                if any(k in err for k in ("ordernotfound", "-2011", "does not exist", "unknown order")):
                    updates[rid] = {"exec_status": ST_CANCELED, "update_time": now_str,
                                    "error_msg": f"撤单查无此单, 强制核销: {e}"}
                    canceled += 1
                    logger.warning(f"[RECON] ⚠️ 撤单查无此单, 强制核销 | OID:{oid}")
                else:
                    updates[rid] = {"update_time": now_str, "error_msg": f"超时撤单失败: {e}"}
                    logger.warning(f"[RECON] 超时撤单失败 | OID:{oid} | {e}")
            continue

        # 常规回填: 仍是无成交挂单则跳过; 否则回填状态与成交
        new_status = map_exchange_status(ccxt_status)
        if new_status == ST_PENDING and filled <= 0:
            continue
        upd = {"update_time": now_str}
        if new_status != ST_PENDING:
            upd["exec_status"] = new_status
        if filled > 0:
            upd["filled_amount"] = filled
            upd["actual_fill_price"] = avg
        updates[rid] = upd
        synced += 1

    ledger.apply_updates(updates)
    if synced or canceled:
        logger.info(f"[RECON] 完成 | 同步:{synced}笔 | 撤销:{canceled}笔 | 耗时:{(time.perf_counter() - t0) * 1000:.0f}ms")


def check_position_consistency(ledger, position_cache):
    """
    持仓一致性校验 (只告警不自动调整): 账本预期持仓 vs 交易所实际持仓。
    预期口径与平仓溯源一致 —— 只累计 filled_amount>0 且未平仓的开仓;
    仅在"账本预期显著大于交易所实际"时告警 (交易所多出的量可能来自网格/手动, 属正常)。
    """
    df = ledger.read()
    if df.empty:
        return
    closed = _closed_open_ids(df)
    opens = df[df["event"].astype(str).str.strip().str.upper() == "OPEN"]

    expected = {}
    for _, r in opens.iterrows():
        if str(r["record_id"]) in closed:
            continue
        amt = to_num(r["filled_amount"])
        if amt <= 0:
            continue
        key = f"{str(r['symbol']).strip()}_{str(r['direction']).strip().upper()}"
        expected[key] = expected.get(key, 0.0) + amt

    for key, exp in expected.items():
        actual = abs(position_cache.get(key, 0.0))
        if actual < exp * (1 - POSITION_DIFF_TOLERANCE):
            logger.warning(f"[RECON] ⚠️ 持仓差异 | {key} | 账本预期:{exp:.6f} > 交易所:{actual:.6f} (不自动调整)")


def preload_account_state(exchange, ledger):
    """
    执行前置快照: 拉权益/持仓/挂单 → 对账 PENDING → 校验持仓一致性。
    权益软兜底(拉取失败沿用上轮值, 不丢轮); 持仓/挂单硬校验(失败返回 None, 上层放弃本轮)。
    """
    global _last_equity
    t0 = time.perf_counter()
    try:
        exchange.load_time_difference()
    except Exception:
        pass

    # 权益: 软兜底, 短时抖动不该错过整轮
    equity = 0.0
    for _ in range(API_MAX_RETRY):
        st, val = get_total_equity(exchange)
        if st == ExecStatus.OK and val > 0:
            equity = _last_equity = val
            break
        time.sleep(1)
    if equity <= 0:
        equity = _last_equity
        logger.warning(f"[PRELOAD] ⚠️ 权益拉取失败, 沿用上轮值:{equity:.2f}")

    # 持仓 / 挂单: 硬校验, 任一为 None 上层将放弃本轮
    position_cache = _retry_fetch("持仓", lambda: _fetch_positions(exchange))
    open_order_cache = _retry_fetch("挂单", lambda: _fetch_open_orders(exchange))

    # 对账与一致性校验 (依赖上述缓存, 缺失则跳过)
    if open_order_cache is not None:
        reconcile_ledger(exchange, ledger, open_order_cache)
    if position_cache is not None:
        check_position_consistency(ledger, position_cache)

    pos_n = "N/A" if position_cache is None else len(position_cache)
    ord_n = "N/A" if open_order_cache is None else sum(len(v) for v in open_order_cache.values())
    logger.info(f"[PRELOAD] 账户快照 | 权益:{equity:.2f} | 持仓:{pos_n}项 | 挂单:{ord_n}笔 | 耗时:{(time.perf_counter() - t0) * 1000:.0f}ms")
    return equity, position_cache, open_order_cache


# ==========================================
# L5. 信号执行 (开/平; 单元隔离 + 实事求是)
# ==========================================
def handle_open(exchange, ledger, ledger_df, sig, total_equity, open_order_cache):
    """
    开仓: 忠实执行当前信号, 不被历史账本状态一票否决。
    幂等去重拦截重复发单; 本单元若有未平开仓仅刺眼告警不阻断; 下单后只记请求状态, 绝不臆测持仓。
    """
    ssd = sig["ssd_key"]
    ssd_df = _filter_ssd(ledger_df, sig)  # 隔离: 仅读本单元切片

    # 幂等: 同前缀(同一条信号)已挂单则拦截 (去重, 非历史依赖)
    if _has_pending_order(open_order_cache, sig):
        logger.warning(f"[OPEN] 拦截(重复挂单) | 前缀:{sig['prefix']}")
        return

    # 历史异常仅告警: 上一对信号疑似未闭环, 但仍按当前信号如实执行
    if _has_unclosed_open(ssd_df):
        logger.warning(f"[OPEN] ⚠️ 本单元存在未平开仓(疑似上一对信号未闭环), 仍如实执行 | {ssd}")

    # 算量: 权益 x 杠杆 x 权重, 夹在下单金额上下限内
    target_value = min(max(total_equity * LEVERAGE * sig["max_weight"], MIN_ORDER_VALUE), MAX_ORDER_VALUE)
    amount = float(exchange.amount_to_precision(sig["symbol"], target_value / sig["price"]))
    if amount <= 0:
        logger.warning(f"[OPEN] 算量为 0, 跳过 | {ssd}")
        return

    result = execute_order(
        exchange=exchange, symbol=sig["symbol"], side=sig["action"], amount=amount,
        client_oid=sig["client_oid"], order_type="market",
        reduce_only=False, position_side=sig["direction"]
    )
    status = ST_PENDING if result.status == ExecStatus.OK else ST_FAILED

    ledger.append(make_record(
        sig, target_amount=amount, target_value=target_value, status=status,
        client_oid=sig["client_oid"], exchange_oid=result.exchange_oid, error_msg=result.error_msg
    ))
    if result.status == ExecStatus.OK:
        _cache_order(open_order_cache, sig["symbol"], result.exchange_oid, sig["client_oid"])
    logger.info(f"[OPEN] {ssd} | 数量:{amount} | 金额:{target_value:.2f} | {status} | CID:{sig['client_oid']}")


def handle_close(exchange, ledger, ledger_df, sig, position_cache, open_order_cache):
    """
    平仓: 只对"本原子单元"负责, 实事求是。
    平仓量 = min(本单元开仓实际成交量, 交易所该方向实际持仓); 溯源与判断只依据本单元数据, 历史异常仅刺眼告警。
    """
    ssd = sig["ssd_key"]
    ssd_df = _filter_ssd(ledger_df, sig)  # 隔离: 仅读本单元切片

    # 溯源: 本单元最近一条"有成交且未平仓"的开仓 (只认成交量, 不纠结状态)
    open_rec = _find_open_to_close(ssd_df)
    if open_rec is None:
        logger.info(f"[CLOSE] 忽略(本单元无可平开仓) | {ssd}")
        return
    linked_id = str(open_rec["record_id"])
    ledger_open_amt = to_num(open_rec["filled_amount"])  # _find_open_to_close 已保证 > 0

    # 实事求是: 一切以交易所该方向实际持仓为准
    actual_pos = abs(position_cache.get(sig["pos_key"], 0.0))
    if actual_pos <= 0:
        # 交易所已无持仓(疑似手动/其它途径平仓) → 不发单, 逻辑核销闭环
        ledger.append(make_record(
            sig, target_amount=0, target_value=0, status=ST_MANUAL_CLOSED,
            client_oid=sig["client_oid"], exchange_oid="",
            error_msg="平仓时交易所无持仓, 逻辑核销", linked_open_id=linked_id
        ))
        logger.warning(f"[CLOSE] ⚠️ 无持仓核销 | {ssd} | 开仓ID:{linked_id}")
        return

    # 实际持仓不足以覆盖本单元开仓量 → 刺眼告警(有外力减仓/账实不符), 不阻断, 以实际持仓封顶
    if actual_pos < ledger_open_amt * (1 - POSITION_DIFF_TOLERANCE):
        logger.warning(
            f"[CLOSE] ⚠️ 实际持仓不足 | {ssd} | 账本开仓量:{ledger_open_amt:.6f} > 交易所持仓:{actual_pos:.6f} | 以实际持仓封顶")

    amount = float(exchange.amount_to_precision(sig["symbol"], min(ledger_open_amt, actual_pos)))
    if amount <= 0:
        logger.warning(f"[CLOSE] 算量为 0, 跳过 | {ssd}")
        return

    result = execute_order(
        exchange=exchange, symbol=sig["symbol"], side=sig["action"], amount=amount,
        client_oid=sig["client_oid"], order_type="market",
        reduce_only=False, position_side=sig["direction"]  # reduce_only + 持仓方向, 严格只减仓
    )
    status = ST_PENDING if result.status == ExecStatus.OK else ST_FAILED

    ledger.append(make_record(
        sig, target_amount=amount, target_value=amount * sig["price"], status=status,
        client_oid=sig["client_oid"], exchange_oid=result.exchange_oid,
        error_msg=result.error_msg, linked_open_id=linked_id  # 关联开仓 ID, 开平一一对应
    ))
    if result.status == ExecStatus.OK:
        _cache_order(open_order_cache, sig["symbol"], result.exchange_oid, sig["client_oid"])
    logger.info(f"[CLOSE] {ssd} | 数量:{amount} | {status} | 开仓ID:{linked_id} | CID:{sig['client_oid']}")


def execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache, ledger):
    """单信号分发: 每次读取最新账本(以看见本轮内已落盘的开仓), 按 OPEN/CLOSE 路由"""
    if position_cache is None or open_order_cache is None:
        logger.error("[EXEC] 账户缓存缺失, 跳过信号")
        return
    sig = parse_signal(row)
    ledger_df = ledger.read()
    if sig["event"] == "OPEN":
        handle_open(exchange, ledger, ledger_df, sig, total_equity, open_order_cache)
    elif sig["event"] == "CLOSE":
        handle_close(exchange, ledger, ledger_df, sig, position_cache, open_order_cache)
    else:
        logger.warning(f"[EXEC] 未知事件类型: {sig['event']} | {sig['ssd_key']}")


def execute_signals(exchange, target_time, total_equity, position_cache, open_order_cache, signal_df, ledger):
    """只处理距目标时间 ±SIGNAL_WINDOW_MIN 分钟内的信号; 逐笔执行, 单信号异常被就地拦截互不影响"""
    t0 = time.perf_counter()
    lower = target_time - timedelta(minutes=SIGNAL_WINDOW_MIN)
    upper = target_time + timedelta(minutes=SIGNAL_WINDOW_MIN)

    times = pd.to_datetime(signal_df["time"], errors="coerce")
    # 时区对齐: Series 若带时区则整体剥离, 与 naive 的 lower/upper 安全比较
    if getattr(times.dt, "tz", None) is not None:
        times = times.dt.tz_localize(None)

    valid = signal_df[(times >= lower) & (times <= upper)]
    if valid.empty:
        logger.info(f"[EXEC] 无有效信号(±{SIGNAL_WINDOW_MIN}min 窗口)")
        return

    for _, row in valid.iterrows():
        try:
            execute_single_signal(exchange, row, total_equity, position_cache, open_order_cache, ledger)
        except Exception as e:
            # 单个原子单元的异常在此彻底拦截, 绝不波及其它单元
            logger.error(f"[EXEC] 单信号异常拦截 | {e}\n{traceback.format_exc()}")

    logger.info(f"[EXEC] 本轮完成 | 共 {len(valid)} 笔 | 耗时:{(time.perf_counter() - t0) * 1000:.1f}ms")


# ==========================================
# L6. 高可用调度器 (顶层流程编排)
# ==========================================



def run_scheduler():
    """顶层编排: 每整点驱动一轮 —— 预加载对账 → 拉信号 → 窗口内执行; 任何环节异常都不致整体停摆"""
    api_key = get_config("myself_biance_api_key")
    secret_key = get_config("myself_biance_api_secret")

    if platform.system().lower() == "linux":
        proxies, proxy_url = None, None
    else:
        proxies = {"http": "http://127.0.0.1:7890", "https": "http://127.0.0.1:7890"}
        proxy_url = "http://127.0.0.1:7890"

    exchange = safe_init_exchange(api_key, secret_key, proxies)
    ledger = LedgerManager(LEDGER_FILE)
    logger.info("[SCHED] 调度系统就绪, 进入整点循环")

    while True:
        try:
            now = datetime.now()
            next_hour = (now + timedelta(hours=1)).replace(minute=0, second=0, microsecond=0)
            target_time_str = (next_hour - timedelta(minutes=1)).strftime("%Y-%m-%d %H:%M")
            preload_time = next_hour - timedelta(minutes=PRELOAD_AHEAD_MIN)

            if now < preload_time:
                time.sleep((preload_time - now).total_seconds())

            # 预加载 + 对账 (最多 API_MAX_RETRY 次; 持仓/挂单彻底失败则放弃本轮, 拒绝脏数据交易)
            equity, position_cache, open_order_cache = 0.0, None, None
            ok = False
            for _ in range(API_MAX_RETRY):
                try:
                    equity, position_cache, open_order_cache = preload_account_state(exchange, ledger)
                    if equity > 0 and position_cache is not None and open_order_cache is not None:
                        ok = True
                        break
                except Exception as e:
                    logger.error(f"[PRELOAD] 异常: {e}")
                time.sleep(3)

            if not ok:
                logger.error("[PRELOAD] 连续失败, 放弃本轮调度, 等待下一整点")
                time.sleep(60)
                continue

            # 拉取信号 → 窗口内执行
            signal_df = execute_trading_bot_workflow(target_time_str, proxy_url=proxy_url)
            if signal_df is not None and not signal_df.empty:
                execute_signals(exchange, next_hour, equity, position_cache, open_order_cache, signal_df, ledger)

        except Exception:
            logger.error(f"[SCHED] 致命异常, 30s 后恢复\n{traceback.format_exc()}")
            time.sleep(30)


if __name__ == "__main__":
    run_scheduler()