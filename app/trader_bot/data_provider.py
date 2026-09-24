# -*- coding: utf-8 -*-
"""
================================================================================
 币安永续合约「K线 / 资金费率 / 未平仓量(OI)」极速数据基建
================================================================================
[功能摘要]
  面向实盘策略的三合一行情拉取引擎：以「本地缓存 + REST 历史补齐 + WS/REST 实时双擎竞速」
  的方式，在目标 K 线收盘的瞬间交付完整历史切片；并为 K 线提供跨进程 Single-Flight 去重，
  同机多进程同参请求只真实打一次网络。

[输入数据]
  · symbol_list      : ccxt 统一符号列表，如 ["BTC/USDC:USDC"]
  · timeframe / days : 周期字符串（'1m'/'15m'/'1h'…，月线 M 不支持指纹对齐）与回溯天数（支持 float）
  · target_time_str  : 目标时间（北京时间口径，会被「数学级向下对齐」到周期边界）
  · 本地缓存 CSV     : data/{SYM}_{tf}_latest.csv、{SYM}_funding_latest.csv、{SYM}_oi_{tf}_latest.csv
  · 结果快照与锁     : data/_snapshots/{key}.meta.json + {key}.pkl；data/_locks/*.lock

[数据流转 / 交互]
  1) 去重(仅K线)：请求参数 → md5 指纹 key → L1 无锁读快照直出 → L2 抢跨进程文件锁 + 双重检查
     → Leader 真实拉取并同步写快照，Follower 复用；等锁超时降级为独立拉取（永不失败）。
  2) 缓存装载：CSV --(跨进程读写锁保护)--> memory_pool{symbol: {ts: kline}}，同时算出每币增量起点 since。
  3) 生产者 → asyncio.Queue：REST 历史分页(HIST-1) / 收盘前缺口二次补齐(HIST-2) /
     WS 实时推送 / 收盘前最后 5s 的无延迟脉冲 REST。
  4) 消费者 data_processor：写入 memory_pool（ts 为 key 天然去重），并按「WS 收线 / 下一根出现 /
     物理时钟强判」三重条件逐币点名，全员到达即放行主线程。
  5) 主线程 O(1) 切片 [start, target] → DataFrame 交付；全量内存池拷贝丢给后台守护线程做
     排序 / merge-on-write 合并 / 原子覆盖落盘（不会覆盖其它进程刚写入的行）。

[输出数据]
  · 返回值：{symbol: DataFrame}
      K线     → [timestamp, open, high, low, close, volume]
      资金费率 → [timestamp, fundingRate, symbol]
      OI      → [timestamp, oi_amount]
  · 副作用：CSV 缓存原子更新、结果快照提交、后台 GC 清理过期快照与僵尸锁、结构化运行日志。
================================================================================
"""

import asyncio
import glob
import hashlib
import json
import os
import pickle
import re
import threading
import time
import uuid
import weakref
from datetime import datetime, timedelta

import aiohttp
import ccxt.async_support as ccxt
import pandas as pd

from common_utils import setup_logger

# 解除 Pandas 控制台打印限制（便于人工核对数据）
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

logger = setup_logger()

# ----------------------------- 全局常量 -----------------------------
MS_PER_DAY = 86_400_000
KLINE_COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
FUNDING_COLS = ['timestamp', 'fundingRate', 'symbol']
OI_COLS = ['timestamp', 'oi_amount']

MAX_CACHE_ROWS = 525_600  # 单币缓存行数上限（≈1 年 1m K 线），防磁盘无限膨胀
HARD_DEADLINE_MS = 60_000  # 目标收盘后最多再死等 60s，随后硬熔断交卷
EXCHANGE_TIMEOUT_MS = 15_000  # 单请求超时，放宽以吸收网络小抖动（抵御 WinError 64 闪断）
_TF_UNIT_MS = {'s': 1000, 'm': 60_000, 'h': 3_600_000, 'd': 86_400_000, 'w': 604_800_000}

# 跨进程去重基建
_SNAPSHOT_VERSION = 2
_SNAPSHOT_DIRNAME = "_snapshots"
_LOCK_DIRNAME = "_locks"
_GC_MIN_INTERVAL_SEC = 600  # GC 节流：同一目录最多 10 分钟扫一次


# =====================================================================
# 🧰 模块零：通用工具（时间 / 路径 / 重试 / 交易所生命周期）
# =====================================================================
def _format_bj_time(ts_ms):
    """毫秒时间戳 → 北京时间字符串。全局唯一时间口径，彻底消除日志中的时区歧义。"""
    return pd.to_datetime(ts_ms, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')


def _cache_path(cache_dir, symbol, tail):
    """缓存文件路径拼装：symbol 中的 / 与 : 一律替换为 _，规避非法文件名"""
    return os.path.join(cache_dir, f"{symbol.replace('/', '_').replace(':', '_')}_{tail}")


def _parse_timeframe_ms(timeframe):
    """本地解析周期毫秒（不依赖 ccxt 实例，避免为了算指纹而先建连交易所）"""
    matched = re.fullmatch(r'\s*(\d+)\s*([smhdwM])\s*', str(timeframe))
    if not matched:
        raise ValueError(f"无法解析的 timeframe: {timeframe}")
    num, unit = int(matched.group(1)), matched.group(2)
    if unit == 'M':
        raise ValueError("月线(M)无固定毫秒长度，不支持指纹级对齐")
    return num * _TF_UNIT_MS[unit]


def _align_target_window(tf_ms, days, target_time_str):
    """
    时间窗口对齐（指纹计算与真实拉取共用同一套数学，否则 10:03 / 10:07 的 15m 请求
    会散列成两个 key，跨进程去重直接失效）。
    出参：(raw_ms 原始, target_ms 向下对齐, start_ms 起点, close_ms 目标收盘)
    """
    ts = pd.to_datetime(target_time_str)
    ts = ts.tz_localize('Asia/Shanghai') if ts.tzinfo is None else ts.tz_convert('Asia/Shanghai')
    raw_ms = int(ts.value // 1_000_000)
    target_ms = raw_ms - (raw_ms % tf_ms)
    return raw_ms, target_ms, target_ms - int(float(days) * MS_PER_DAY), target_ms + tf_ms


def _ensure_no_running_loop():
    """卫语句：同步入口禁止在已运行的事件循环内调用，否则 asyncio.run 必然爆炸"""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return
    if loop.is_running():
        raise RuntimeError("检测到已存在运行中的异步事件循环。\n请在顶部执行：import nest_asyncio; nest_asyncio.apply()")


async def _retry_async(action, what, log_prefix="", attempts=3, delay=2.0, expo=False):
    """
    统一异步重试装甲（全链路防断网核心）。抵御代理抖动 / WinError 64 闪断 / 交易所瞬时限流。
    action  : 无参协程工厂，例如 lambda: exchange.fetch_ohlcv(...)
    行为    : 重试耗尽后抛出最后一次异常，由调用方决定「跳过该币」还是「上抛终止」。
    """
    last_err = None
    for i in range(1, max(1, attempts) + 1):
        try:
            return await action()
        except asyncio.CancelledError:
            raise
        except Exception as e:
            last_err = e
            if i >= attempts:
                break
            wait = delay * (2 ** (i - 1)) if expo else delay
            logger.warning(f"{log_prefix} [RETRY] ⚠️ {what} 失败 | 第=[{i}/{attempts}]次 错误=[{e}] "
                           f"| [{wait:.1f}s] 后重试（可能原因：网络闪断 / 代理不稳 / 交易所限流）")
            await asyncio.sleep(wait)
    logger.error(f"{log_prefix} [RETRY] ❌ {what} 连续 [{attempts}] 次全部失败，放弃本次请求 | 末次错误=[{last_err}]")
    raise last_err


async def _shutdown_exchange(exchange):
    """
    交易所连接强杀式脱壳：先撕裂底层 connector，再骗过 CCXT 析构检查，避免退出期长篇警告。
    ⚠️ 按原设计静默吞掉退出阶段的一切异常（此时物理连接已断，报错对业务无意义）。
    """
    try:
        session = getattr(exchange, 'session', None)
        connector = getattr(session, 'connector', None) if session else None
        if connector:
            try:
                await asyncio.wait_for(connector.close(), timeout=0.00002)
            except Exception:
                pass
        exchange.session = None
        await asyncio.wait_for(exchange.close(), timeout=0.00002)
    except Exception:
        pass


async def _open_exchange(proxy_url, tag, log_prefix=""):
    """
    统一建连币安永续（defaultType=swap）：按需注入代理、放宽超时，并为 load_markets 覆盖
    指数退避重试。初始化失败会先释放连接再上抛，绝不泄漏 session。
    """
    config = {'enableRateLimit': True, 'options': {'defaultType': 'swap'}, 'timeout': EXCHANGE_TIMEOUT_MS}
    if proxy_url:
        config['aiohttp_proxy'] = proxy_url
        config['proxies'] = {'http': proxy_url, 'https': proxy_url}

    exchange = ccxt.binance(config)
    try:
        await _retry_async(lambda: exchange.load_markets(), f"[{tag}] 交易所市场元数据加载(load_markets)",
                           log_prefix, attempts=3, delay=1.0, expo=True)
    except Exception:
        logger.error(f"{log_prefix} [{tag}] ❌ 交易所初始化失败，本次任务无法开工 "
                     f"| 可能原因：代理未启动 / 网络不通 / 币安封禁当前出口 IP，异常上抛给上层接管")
        await _shutdown_exchange(exchange)
        raise
    return exchange


# =====================================================================
# 🔐 模块一：跨进程去重基建（Inter-Process Single-Flight）
# ---------------------------------------------------------------------
# 目标：同机多进程发起「完全同参」K 线请求时，只允许一个进程真实打网络（Leader），
#      其余进程等待并复用其结果（Follower）。
# 分层：L1 无锁快照直出 → L2 文件锁 + 双重检查 → L3 等锁超时降级自取（永不失败）
# =====================================================================
try:
    from filelock import FileLock as _FileLockImpl  # 优先使用久经考验的 filelock

    _HAS_FILELOCK = True
except Exception:
    _FileLockImpl = None
    _HAS_FILELOCK = False


class _RawFileLock:
    """
    标准库 fallback 文件锁（无 filelock 依赖时启用）。
    POSIX 走 fcntl.flock，Windows 走 msvcrt.locking，均为内核级锁：
    进程崩溃或被强杀时由操作系统自动回收，不存在「死锁文件」残留问题。
    """

    def __init__(self, lock_file):
        self.lock_file = lock_file
        self._fd = None

    def try_acquire(self):
        if self._fd is not None:
            return True
        fd = None
        try:
            fd = os.open(self.lock_file, os.O_RDWR | os.O_CREAT, 0o644)
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(fd, msvcrt.LK_NBLCK, 1)
            else:
                import fcntl
                fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
            self._fd = fd
            return True
        except Exception:
            if fd is not None:
                try:
                    os.close(fd)
                except Exception:
                    pass
            return False

    def release(self):
        if self._fd is None:
            return
        try:
            if os.name == 'nt':
                import msvcrt
                msvcrt.locking(self._fd, msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fd, fcntl.LOCK_UN)
        except Exception:
            pass
        try:
            os.close(self._fd)
        except Exception:
            pass
        self._fd = None


class _FileLockAdapter:
    """把 filelock.FileLock 适配成统一的 try_acquire / release 接口"""

    def __init__(self, lock_file):
        self._lock = _FileLockImpl(lock_file, timeout=0)

    def try_acquire(self):
        try:
            self._lock.acquire(timeout=0)
            return True
        except Exception:
            return False

    def release(self):
        try:
            self._lock.release()
        except Exception:
            pass


class _LockWrapper:
    """包装 threading.Lock 使其支持弱引用，从而可被 WeakValueDictionary 自动回收"""

    def __init__(self):
        self.lock = threading.Lock()

    def acquire(self, *args, **kwargs):
        return self.lock.acquire(*args, **kwargs)

    def release(self):
        return self.lock.release()


# 弱引用字典：长生命周期进程下不会因锁路径无限增长而内存泄漏
_THREAD_LOCKS = weakref.WeakValueDictionary()
_THREAD_LOCKS_GUARD = threading.Lock()


def _get_thread_lock(abs_path):
    """同进程内同一把文件锁路径共享一个线程锁，规避 flock 同进程双 FD 的语义坑"""
    with _THREAD_LOCKS_GUARD:
        lock = _THREAD_LOCKS.get(abs_path)
        if lock is None:
            lock = _LockWrapper()
            _THREAD_LOCKS[abs_path] = lock
        return lock


class InterProcessMutex:
    """
    跨进程 + 跨线程双层互斥锁。
    设计要点：acquire() 永不抛超时异常，只返回 True/False，把降级策略交还调用方；
             支持排队心跳回调 on_wait，避免长时间等待时日志「假死」。
    """

    def __init__(self, lock_path):
        self.lock_path = os.path.abspath(lock_path)
        self._tlock = _get_thread_lock(self.lock_path)
        self._tlock_held = False
        self._flock = None
        self._flock_held = False

    def acquire(self, timeout=-1, poll_interval=0.2, on_wait=None, wait_log_interval=15.0):
        t_start = time.monotonic()
        deadline = None if (timeout is None or timeout < 0) else t_start + timeout

        # 1) 线程级互斥
        if deadline is None:
            got_thread = self._tlock.acquire(True)
        else:
            got_thread = self._tlock.acquire(True, max(0.0, deadline - time.monotonic()))
        if not got_thread:
            return False
        self._tlock_held = True

        # 2) 进程级互斥：轮询式非阻塞抢占，便于打心跳并精准控制超时
        try:
            parent = os.path.dirname(self.lock_path)
            if parent:
                os.makedirs(parent, exist_ok=True)
            self._flock = _FileLockAdapter(self.lock_path) if _HAS_FILELOCK else _RawFileLock(self.lock_path)

            last_log = t_start
            while True:
                if self._flock.try_acquire():
                    self._flock_held = True
                    return True
                now = time.monotonic()
                if deadline is not None and now >= deadline:
                    self._release_thread_lock()
                    return False
                if on_wait is not None and (now - last_log) >= wait_log_interval:
                    last_log = now
                    try:
                        on_wait(now - t_start)
                    except Exception:
                        pass
                time.sleep(poll_interval)
        except Exception as e:
            logger.warning(f"[MUTEX] ⚠️ 进程锁抢占异常，视为未取得锁（后续将降级为独立执行） "
                           f"| 锁=[{self.lock_path}] 错误=[{e}]")
            self._release_thread_lock()
            return False

    def _release_thread_lock(self):
        if not self._tlock_held:
            return
        try:
            self._tlock.release()
        except Exception:
            pass
        self._tlock_held = False

    def release(self):
        if self._flock_held and self._flock is not None:
            self._flock.release()
            self._flock_held = False
        self._release_thread_lock()

    def __enter__(self):
        self.acquire()
        return self

    def __exit__(self, *exc):
        self.release()


def _atomic_replace(src, dst, retries=12, delay=0.15, log_prefix=""):
    """
    带退避重试的原子覆盖。Windows 下目标文件若正被其它进程 open 读取，os.replace 会抛
    PermissionError，此处重试兜底；彻底失败则清理临时文件并返回 False（不影响主流程交付）。
    """
    last_err = None
    for _ in range(max(1, retries)):
        try:
            os.replace(src, dst)
            return True
        except Exception as e:
            last_err = e
            time.sleep(delay)
    logger.error(f"{log_prefix} [IO] ❌ 原子覆盖失败，本次落盘作废（下次运行会重新回拉这段数据） "
                 f"| 目标=[{dst}] 错误=[{last_err}]（可能原因：文件被其它进程长时间占用 / 磁盘只读）")
    try:
        if os.path.exists(src):
            os.remove(src)
    except Exception:
        pass
    return False


def _csv_rw_lock_path(path):
    return f"{path}.rw.lock"


def _read_csv_guarded(path, timeout=5.0, log_prefix=""):
    """
    加同一把读写锁读取 CSV，与后台落盘线程严格串行，规避「读到半截文件」与 Windows replace 冲突。
    抢不到锁时仍尽力读取（best-effort），绝不阻塞主链路。
    ⚠️ 本函数内部会抢锁，禁止在已持有同一把锁的代码块中调用（线程锁不可重入）。
    """
    mutex = InterProcessMutex(_csv_rw_lock_path(path))
    got_lock = mutex.acquire(timeout=timeout)
    try:
        last_err = None
        for _ in range(3):
            try:
                return pd.read_csv(path)
            except Exception as e:
                last_err = e
                time.sleep(0.1)
        raise last_err
    finally:
        if got_lock:
            mutex.release()


def _build_kline_request_signature(symbol_list, timeframe, days, target_time_str):
    """
    构建请求指纹（跨进程去重的唯一 key）。
    指纹刻意 **不含** days / use_ws / use_rest / proxy_url：
      · days 存入 meta，实现「大范围快照被小范围请求切片复用」
      · 传输方式不影响数据内容，纳入 key 只会降低命中率
    出参核心 Key：key / timeframe / timeframe_ms / days / target_time_ms / start_time_ms /
                 target_close_time_ms / symbols(已排序去重)
    """
    tf_ms = _parse_timeframe_ms(timeframe)
    _, target_time_ms, start_time_ms, target_close_time_ms = _align_target_window(tf_ms, days, target_time_str)

    symbols = sorted({str(s).strip() for s in symbol_list})
    sym_hash = hashlib.md5("|".join(symbols).encode('utf-8')).hexdigest()[:12]
    tf_tag = re.sub(r'[^0-9A-Za-z]', '', str(timeframe))

    return {
        'key': f"kline_{tf_tag}_{target_time_ms}_{sym_hash}",
        'timeframe': str(timeframe),
        'timeframe_ms': tf_ms,
        'days': days,
        'target_time_ms': target_time_ms,
        'start_time_ms': start_time_ms,
        'target_close_time_ms': target_close_time_ms,
        'symbols': symbols,
    }


def _snapshot_paths(snapshot_dir, key):
    return (os.path.join(snapshot_dir, f"{key}.meta.json"),
            os.path.join(snapshot_dir, f"{key}.pkl"))


def _read_kline_snapshot(sig, snapshot_dir, ttl_sec=None, incomplete_ttl_sec=600, log_prefix=""):
    """
    读取并严格校验结果快照，任何一环不满足即视为 miss（宁可重拉，绝不返回可疑数据）。
    校验链：meta/pkl 均在 → 版本+周期+目标时间+币种全量一致 → 历史深度足够 → TTL
            → pkl 可反序列化 → 逐币硬复核（必须含 target 那根 & 起点无脱节）
    出参形貌：{symbol: DataFrame[KLINE_COLS]}（symbol 为归一化后的名字）或 None
    """
    meta_path, data_path = _snapshot_paths(snapshot_dir, sig['key'])
    if not (os.path.exists(meta_path) and os.path.exists(data_path)):
        return None

    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
        if int(meta.get('version', -1)) != _SNAPSHOT_VERSION:
            return None
        if str(meta.get('timeframe')) != str(sig['timeframe']):
            return None
        if int(meta.get('target_time_ms', -1)) != int(sig['target_time_ms']):
            return None
        if list(meta.get('symbols') or []) != sig['symbols']:
            return None
        snap_start_ms = int(meta['start_time_ms'])
        created_at = float(meta.get('created_at', 0))
    except Exception:
        return None

    if snap_start_ms > sig['start_time_ms']:
        return None  # 快照历史深度不够，无法满足本次 days

    age = max(0.0, time.time() - created_at)
    if ttl_sec is not None and age > float(ttl_sec):
        return None
    if not bool(meta.get('complete', False)) and age > float(incomplete_ttl_sec):
        logger.info(f"{log_prefix} 🧹 快照标记为不完整且已超出保护期，放弃复用并重新补洞 | age=[{age:.0f}s]")
        return None

    try:
        with open(data_path, 'rb') as f:
            payload = pickle.load(f)
        data = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None
    except Exception as e:
        logger.warning(f"{log_prefix} ⚠️ 快照数据文件损坏或不可读，视为未命中并重新拉取 | 错误=[{e}]")
        return None

    tf_ms = sig['timeframe_ms']
    need_slice = snap_start_ms < sig['start_time_ms']
    out, payload_complete = {}, True

    for sym in sig['symbols']:
        df = data.get(sym)
        if not isinstance(df, pd.DataFrame) or 'timestamp' not in df.columns:
            return None  # 币种缺失 = 结构性失配，直接 miss
        if need_slice and not df.empty:
            df = df[df['timestamp'] >= sig['start_time_ms']].reset_index(drop=True)
        # 硬复核：目标那根必须在场，且起点不得脱节超过 1 根
        if df.empty or int(df['timestamp'].max()) != sig['target_time_ms'] \
                or (int(df['timestamp'].min()) - sig['start_time_ms']) > tf_ms:
            payload_complete = False
        out[sym] = df

    if not payload_complete and age > float(incomplete_ttl_sec):
        logger.info(f"{log_prefix} 🧹 快照实测存在断缺且超出保护期，放弃复用 | age=[{age:.0f}s]")
        return None
    return out


def _write_kline_snapshot(sig, final_dfs, snapshot_dir, log_prefix=""):
    """
    Leader 交付前同步写快照（必须同步：Follower 是在 Leader 释放锁之后才做双重检查的）。
    写序：先原子写 pkl（数据体）→ 再原子写 meta.json（提交标记），确保不会出现「有 meta 无数据」。
    入参形貌：final_dfs={symbol: DataFrame[KLINE_COLS]}
    """
    t0, size_mb = time.time(), 0.0
    os.makedirs(snapshot_dir, exist_ok=True)
    meta_path, data_path = _snapshot_paths(snapshot_dir, sig['key'])

    expected_rows = int((sig['target_time_ms'] - sig['start_time_ms']) / sig['timeframe_ms']) + 1
    normalized = {str(k).strip(): v for k, v in (final_dfs or {}).items()}
    data, rows, complete = {}, {}, True

    for sym in sig['symbols']:
        df = normalized.get(sym)
        if not isinstance(df, pd.DataFrame):
            complete = False
            continue
        data[sym] = df
        rows[sym] = int(len(df))
        if df.empty or len(df) < expected_rows or int(df['timestamp'].max()) != sig['target_time_ms']:
            complete = False

    if len(data) != len(sig['symbols']):
        logger.warning(f"{log_prefix} ⚠️ 结果币种不齐，跳过快照写入以免污染缓存 "
                       f"| 实得=[{len(data)}/{len(sig['symbols'])}]")
        return False

    meta = {
        'version': _SNAPSHOT_VERSION,
        'key': sig['key'],
        'timeframe': sig['timeframe'],
        'timeframe_ms': sig['timeframe_ms'],
        'days': sig['days'],
        'target_time_ms': sig['target_time_ms'],
        'start_time_ms': sig['start_time_ms'],
        'symbols': sig['symbols'],
        'rows': rows,
        'expected_rows': expected_rows,
        'complete': bool(complete),
        'created_at': time.time(),
        'created_at_bj': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        'writer_pid': os.getpid(),
    }

    tmp_data = f"{data_path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp_data, 'wb') as f:
            pickle.dump({'version': _SNAPSHOT_VERSION, 'meta': meta, 'data': data}, f, protocol=4)
        size_mb = os.path.getsize(tmp_data) / (1024 * 1024)
        if not _atomic_replace(tmp_data, data_path, log_prefix=log_prefix):
            return False
    except Exception as e:
        logger.error(f"{log_prefix} ❌ 快照数据体写入失败，本次不提交快照（其它进程会各自拉取） | 错误=[{e}]")
        try:
            if os.path.exists(tmp_data):
                os.remove(tmp_data)
        except Exception:
            pass
        return False

    tmp_meta = f"{meta_path}.{uuid.uuid4().hex}.tmp"
    try:
        with open(tmp_meta, 'w', encoding='utf-8') as f:
            json.dump(meta, f, ensure_ascii=False)
        if not _atomic_replace(tmp_meta, meta_path, log_prefix=log_prefix):
            return False
    except Exception as e:
        logger.error(f"{log_prefix} ❌ 快照 meta 提交失败，快照将被视为未生成 | 错误=[{e}]")
        return False

    # logger.info(f"{log_prefix} 📸 结果快照已提交 | 完整=[{complete}] 体积=[{size_mb:.2f}MB] 耗时=[{time.time() - t0:.3f}s]")
    return True


def _gc_dedupe_dirs(snapshot_dir, lock_dir, keep_sec=86400, log_prefix=""):
    """清理过期快照 / 僵尸 .tmp / 陈旧锁文件。全程 best-effort，绝不影响业务。"""
    now, removed = time.time(), 0

    # 1) 过期快照（历史 K 线是幂等的，过期只为控制磁盘占用）
    for path in glob.glob(os.path.join(snapshot_dir, "kline_*")):
        try:
            if path.endswith('.tmp'):
                continue
            if now - os.path.getmtime(path) > keep_sec:
                os.remove(path)
                removed += 1
        except Exception:
            pass

    # 2) 残留 .tmp 碎片（存活超过 1 小时必属僵尸）
    for pattern in (os.path.join(snapshot_dir, "*.tmp"), os.path.join(lock_dir, "*.tmp")):
        for path in glob.glob(pattern):
            try:
                if now - os.path.getmtime(path) > 3600:
                    os.remove(path)
                    removed += 1
            except Exception:
                pass

    # 3) 陈旧锁文件：仅清理 3 天以上、且能「非阻塞抢到」的锁
    #    （POSIX 下删除正被持有的锁文件会直接破坏互斥语义，绝不能碰）
    for path in glob.glob(os.path.join(lock_dir, "kline_*.lock")):
        try:
            if now - os.path.getmtime(path) <= 3 * 86400:
                continue
            probe = InterProcessMutex(path)
            if probe.acquire(timeout=0):
                try:
                    os.remove(path)
                    removed += 1
                finally:
                    probe.release()
        except Exception:
            pass

    if removed:
        logger.info(f"{log_prefix} 🧹 去重目录 GC 完成 | 清理文件=[{removed}]")


def _maybe_dispatch_dedupe_gc(snapshot_dir, lock_dir, keep_sec, log_prefix=""):
    """节流 + 单进程抢占的后台 GC，绝不占用主链路耗时（异常全静默，属清理性质）"""
    try:
        stamp = os.path.join(snapshot_dir, ".gc_stamp")
        if os.path.exists(stamp) and (time.time() - os.path.getmtime(stamp)) < _GC_MIN_INTERVAL_SEC:
            return
        gc_mutex = InterProcessMutex(os.path.join(lock_dir, "gc.lock"))
        if not gc_mutex.acquire(timeout=0):
            return
        try:
            with open(stamp, 'w') as f:
                f.write(str(time.time()))
        except Exception:
            pass

        def _task():
            try:
                _gc_dedupe_dirs(snapshot_dir, lock_dir, keep_sec, log_prefix)
            finally:
                gc_mutex.release()

        threading.Thread(target=_task, daemon=True).start()
    except Exception:
        pass


# =====================================================================
# 🗄️ 模块二：K 线缓存装载与后台落盘
# =====================================================================
def load_local_cache(symbol_list, start_time_ms, timeframe_ms, timeframe, cache_dir="data", log_prefix=""):
    """
    智能装载 K 线本地缓存，决定每个币真正需要联网补拉的起点。
    命中条件（缺一不可）：缓存覆盖 start_time_ms、[start, max] 区间无空洞、首根与 start 脱节 ≤ 1 根。
      命中 → since = 缓存最大 ts（只补增量）；未命中 → since = start_time_ms（整段回拉补洞）。
    出参形貌：(memory_pool={symbol: {ts: [ts,o,h,l,c,v]}}, fetch_since_map={symbol: since_ms})
    """
    t0 = time.time()
    memory_pool = {sym: {} for sym in symbol_list}
    fetch_since_map = {sym: start_time_ms for sym in symbol_list}
    hits, loaded_rows, broken = 0, 0, []

    for sym in symbol_list:
        path = _cache_path(cache_dir, sym, f"{timeframe}_latest.csv")
        if not os.path.exists(path):
            continue
        try:
            df = _read_csv_guarded(path, timeout=5.0, log_prefix=log_prefix)
            if df.empty or 'timestamp' not in df.columns:
                continue

            # values.tolist() 比 iterrows 快百倍，是主线程不被几十万行缓存拖死的关键
            for row in df[KLINE_COLS].values.tolist():
                ts = int(row[0])
                memory_pool[sym][ts] = [ts] + row[1:]
            loaded_rows += len(df)

            if int(df['timestamp'].min()) > start_time_ms:
                continue
            sub_df = df[df['timestamp'] >= start_time_ms]
            if sub_df.empty:
                continue
            sub_min, sub_max = int(sub_df['timestamp'].min()), int(sub_df['timestamp'].max())
            expected_rows = (sub_max - sub_min) // timeframe_ms + 1
            if len(sub_df) < expected_rows or (sub_min - start_time_ms) > timeframe_ms:
                continue

            fetch_since_map[sym] = int(df['timestamp'].max())
            hits += 1
        except Exception as e:
            broken.append(f"{sym}({e})")

    earliest = _format_bj_time(min(fetch_since_map.values())) if fetch_since_map else 'N/A'
    # logger.info(f"{log_prefix} [CACHE] ♻️ 本地缓存装载完毕 | 命中=[{hits}] 未命中=[{len(symbol_list) - hits}] "
    #             f"载入行数=[{loaded_rows}] 最早增量起点=[{earliest}] 耗时=[{time.time() - t0:.2f}s]")
    if broken:
        logger.warning(f"{log_prefix} [CACHE] ⚠️ 部分缓存读取失败，这些币将整段回拉（可能原因：文件被写坏 / 磁盘异常） "
                       f"| 数量=[{len(broken)}] 明细={broken[:3]}")
    return memory_pool, fetch_since_map


def _write_kline_cache_files(full_dfs, cache_dir, timeframe, log_prefix=""):
    """
    （后台线程专用）K 线缓存落盘：文件级跨进程读写锁 + merge-on-write + 原子覆盖。
    merge-on-write 的意义：磁盘上可能有「其它进程」刚写入的新行，先读出来合并再覆盖，
    timestamp 去重保留本进程内存池的值（keep='last'），彻底消除后写者覆盖前写者的丢帧。
    入参形貌：full_dfs={symbol: DataFrame[KLINE_COLS]}
    """
    t0 = time.time()
    io_size, merged_rows, no_lock, failed = 0, 0, [], []
    os.makedirs(cache_dir, exist_ok=True)

    for symbol, df in full_dfs.items():
        path = _cache_path(cache_dir, symbol, f"{timeframe}_latest.csv")
        mutex = InterProcessMutex(_csv_rw_lock_path(path))
        got_lock = mutex.acquire(timeout=60.0)  # 抢不到锁也不放弃落盘，退化为无锁覆写
        if not got_lock:
            no_lock.append(symbol)

        try:
            out_df = df
            if got_lock and os.path.exists(path):
                try:
                    # 已持锁，必须直连 pd.read_csv（_read_csv_guarded 会再抢同一把锁，线程锁不可重入）
                    old_df = pd.read_csv(path)
                    if not old_df.empty and 'timestamp' in old_df.columns:
                        before = len(df)
                        out_df = (pd.concat([old_df.reindex(columns=KLINE_COLS), df.reindex(columns=KLINE_COLS)],
                                            ignore_index=True)
                                  .dropna(subset=['timestamp'])
                                  .drop_duplicates(subset=['timestamp'], keep='last')
                                  .sort_values('timestamp')
                                  .reset_index(drop=True))
                        if len(out_df) > MAX_CACHE_ROWS:
                            out_df = out_df.iloc[-MAX_CACHE_ROWS:].reset_index(drop=True)
                        merged_rows += max(0, len(out_df) - before)
                except Exception as e:
                    logger.warning(f"{log_prefix} [DISK] ⚠️ {symbol} 旧缓存合并失败，退化为直接覆写 "
                                   f"（可能丢失其它进程刚写入的行） | 错误=[{e}]")
                    out_df = df

            tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
            try:
                out_df.to_csv(tmp_path, index=False)
                io_size += os.path.getsize(tmp_path)
                _atomic_replace(tmp_path, path, log_prefix=log_prefix)
            except Exception as e:
                failed.append(f"{symbol}({e})")
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
        finally:
            if got_lock:
                mutex.release()

    summary = (f"{log_prefix} [DISK] 💾 后台守护落盘完毕 | 文件=[{len(full_dfs)}] 合并新增行=[{merged_rows}] "
               f"体积=[{io_size / (1024 * 1024):.2f}MB] 耗时=[{time.time() - t0:.3f}s]")
    if no_lock:
        summary += f" | ⚠️ 抢锁超时降级覆写=[{len(no_lock)}] {no_lock[:3]}"
    logger.info(summary)
    if failed:
        logger.error(f"{log_prefix} [DISK] ❌ 部分币种缓存落盘失败，下次运行需重新回拉这段历史 "
                     f"| 失败=[{len(failed)}] 明细={failed[:5]}")


def dispatch_kline_background_save(memory_pool_copy, timeframe, cache_dir="data", log_prefix=""):
    """
    主线程金蝉脱壳：把「巨量排序 + DataFrame 构建 + 落盘」整体交给非守护线程
    （daemon=False 保证进程退出前一定写完），主线程立刻交付数据给业务。
    入参形貌：memory_pool_copy={symbol: {ts: [ts,o,h,l,c,v]}}
    """

    def _task():
        try:
            full_dfs = {}
            for sym, pool in memory_pool_copy.items():
                klines = sorted(pool.values(), key=lambda k: k[0])[-MAX_CACHE_ROWS:]
                full_dfs[sym] = pd.DataFrame(klines, columns=KLINE_COLS)
            _write_kline_cache_files(full_dfs, cache_dir, timeframe, log_prefix)
        except Exception as e:
            # 后台线程无异常接收方，按原设计仅告警：本次缓存未更新，但已交付的数据不受影响
            logger.error(f"{log_prefix} [DISK] ❌ 后台落盘流水线崩溃，本轮缓存未更新（不影响已交付数据） | 错误=[{e}]")

    threading.Thread(target=_task, daemon=False).start()


# =====================================================================
# 🧠 模块三：唯一消费者（数据大脑）
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                         completion_event, memory_pool, processor_stats):
    """
    单消费者：把所有生产者塞进队列的 K 线写入 memory_pool（ts 为 key，天然去重），
    并判定每个币是否已「拿到目标那根的收盘价」。三重到达条件任一满足即点名：
      ① WS 明确标记该根已收线  ② 已经出现下一根 K 线  ③ 物理时钟越过收盘 +10s（防流动性枯竭永不收线）
    入参形貌：queue 元素=(symbol, kline[ts,o,h,l,c,v], source, is_closed)；
              processor_stats={reached:set, winners:{源:数}, throughput:{}, details:{}}
    """
    reached_symbols = processor_stats["reached"]
    stats = {"HIST": 0, "WS": 0, "REST_POLL": 0}

    try:
        while True:
            symbol, kline, source, is_closed = await queue.get()
            ts = int(kline[0])
            stats[source] += 1
            memory_pool[symbol][ts] = kline

            if symbol not in reached_symbols:
                closed_by_ws = (ts == target_time_ms and is_closed)
                next_candle_seen = (ts >= target_time_ms + timeframe_ms)
                forced_by_clock = (ts == target_time_ms and time.time() * 1000 > target_time_ms + timeframe_ms + 10000)

                if closed_by_ws or next_candle_seen or forced_by_clock:
                    reached_symbols.add(symbol)
                    processor_stats["winners"][source] += 1
                    mark = "⏱️强判" if forced_by_clock and not (closed_by_ws or next_candle_seen) else ""
                    processor_stats["details"][symbol.split(':')[0]] = \
                        f"{source}{mark}({datetime.now().strftime('%H:%M:%S.%f')[:-3]})"
                    if len(reached_symbols) == len(symbol_list):
                        processor_stats["throughput"] = stats
                        completion_event.set()
            queue.task_done()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        # 防死锁装甲（原设计刻意如此）：大脑一旦阵亡必须解除主线程阻塞，否则整个进程永久挂死
        logger.error(f"{log_prefix_safe(processor_stats)} [PROCESSOR] ❌ 数据大脑发生致命异常，已强制解除全局阻塞锁 "
                     f"| 错误=[{e}]（可能原因：队列元素结构异常 / 出现未登记的数据源）")
        completion_event.set()


def log_prefix_safe(processor_stats):
    """消费者异常日志取 RunID：stats 里带则用，缺失则留空，保证告警本身绝不再抛异常"""
    return processor_stats.get("log_prefix", "")


# =====================================================================
# 🚜 模块四：三路生产者（历史 REST / 实时 WS / 脉冲 REST）
# =====================================================================
async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue, tracker=None):
    """
    REST 历史分页补齐：自 since_ms 前移 1 小时开始（重叠拉取，防边界脱节），逐页塞入队列。
    单页彻底失败即放弃该币剩余分页（尽力而为，交由二次补齐 / 实时引擎兜底）。
    入参形貌：tracker={done,total,max_cost,fetched_rows,latest_ts,phase,log_prefix}，全员交卷时打一条聚合日志。
    """
    start_t = time.time()
    prefix = tracker.get('log_prefix', '') if tracker else ''
    limit, curr_since, total_fetched, latest_ts = 1000, since_ms - 3_600_000, 0, 0

    while True:
        try:
            ohlcvs = await _retry_async(
                lambda: exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit),
                f"[HIST] {symbol} 历史分页拉取(since={_format_bj_time(curr_since)})",
                prefix, attempts=4, delay=2.0)
        except asyncio.CancelledError:
            raise
        except Exception:
            break  # 已在 _retry_async 中告警，此处放弃该币剩余分页
        if not ohlcvs:
            break

        total_fetched += len(ohlcvs)
        latest_ts = ohlcvs[-1][0]
        for kline in ohlcvs:
            await queue.put((symbol, kline, "HIST", False))

        curr_since = latest_ts + 1
        if len(ohlcvs) < limit:
            break

    if tracker is None:
        return
    tracker['done'] += 1
    tracker['max_cost'] = max(tracker.get('max_cost', 0), time.time() - start_t)
    tracker['fetched_rows'] += total_fetched
    tracker['latest_ts'] = max(tracker.get('latest_ts', 0), latest_ts)
    if tracker['done'] < tracker['total']:
        return

    latest_str = _format_bj_time(tracker['latest_ts']) if tracker['latest_ts'] > 0 else 'N/A'
    # logger.info(f"{prefix} [{tracker.get('phase', 'HIST')}] 📦 历史补齐全员交卷 "
    #             f"| 币种=[{tracker['done']}/{tracker['total']}] 新增行数=[{tracker['fetched_rows']}] "
    #             f"最慢单币=[{tracker['max_cost']:.2f}s] 数据最新=[{latest_str}]")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=""):
    """
    WS 实时推送引擎（首要竞速通道）：动态映射 ccxt 符号 ↔ 币安流名，全币种通用无硬编码。
    断线走指数退避自愈重连（2→4→8→16s 封顶），永不主动退出，由编排层 cancel 收尾。
    """
    ws_mapping = {s.replace("/", "").split(":")[0].upper(): s for s in symbol_list}
    streams = "/".join(f"{name.lower()}@kline_{timeframe}" for name in ws_mapping)
    # : 端点为 /market/stream（币安官方组合流文档为 /stream），保留原样以免改变线上既有行为
    stream_url = f"wss://fstream.binance.com/market/stream?streams={streams}"

    attempt = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(stream_url, proxy=proxy_url, heartbeat=10) as ws:
                    # logger.info(f"{log_prefix} [WSS] ✅ 实时数据总线已建连 | 订阅流=[{len(ws_mapping)}] 周期=[{timeframe}]")
                    attempt = 0
                    async for msg in ws:
                        if msg.type != aiohttp.WSMsgType.TEXT:
                            continue
                        payload = json.loads(msg.data).get('data') or {}
                        k_data = payload.get('k')
                        target_symbol = ws_mapping.get(payload.get('s'))
                        if not k_data or target_symbol is None:
                            continue
                        kline = [int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                 float(k_data['l']), float(k_data['c']), float(k_data['v'])]
                        await queue.put((target_symbol, kline, "WS", bool(k_data['x'])))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            sleep_time = min(2 ** attempt, 16)
            logger.error(f"{log_prefix} [WSS] ❌ 实时总线断开，正在自愈重连 | 第=[{attempt}]次 等待=[{sleep_time}s] "
                         f"错误=[{e}]（可能原因：代理抖动 / 网络闪断 / 币安主动踢连）")
            await asyncio.sleep(sleep_time)


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    """
    REST 脉冲轮询兜底（仅在收盘前最后 5s 点火）：以最小延迟抢到收盘那根；
    is_closed 由「该根是否已被更新的一根超越」推断。
    : 刻意不做 sleep（极限竞速潜规则），仅依赖 ccxt enableRateLimit 节流，
           高频/多币场景存在触发交易所限频甚至封 IP 的风险，需业务方确认可接受。
    """
    while True:
        results = await asyncio.gather(*[exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list],
                                       return_exceptions=True)
        for sym, ohlcvs in zip(symbol_list, results):
            if isinstance(ohlcvs, Exception) or not ohlcvs:
                continue
            latest_ts = ohlcvs[-1][0]
            for kline in ohlcvs:
                await queue.put((sym, kline, "REST_POLL", kline[0] < latest_ts))


# =====================================================================
# 🎯 模块五：K 线狙击编排（Orchestrator）
# =====================================================================
def parse_time_params(exchange, timeframe, days, target_time_str):
    """
    解析并对齐时间边界（K 线 / OI 引擎共用）。核心潜规则：target 必须数学级向下对齐到周期边界，
    否则 10:03 请求 15m 会去索要一根根本不存在的 K 线。
    出参：(timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms)
    """
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
    raw_ms, target_ms, start_ms, close_ms = _align_target_window(timeframe_ms, days, target_time_str)
    if raw_ms != target_ms:
        logger.info(f"[TIME] 🛡️ 目标时间不符合周期切片标准，已自动向下对齐 | 周期=[{timeframe}] "
                    f"原始=[{_format_bj_time(raw_ms)}] → 对齐=[{_format_bj_time(target_ms)}] "
                    f"抹除零头=[{raw_ms - target_ms}ms]")
    return timeframe_ms, target_ms, start_ms, close_ms


async def _async_core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str,
                                           use_ws, use_rest, proxy_url):
    """
    K 线狙击主编排。链路：建连 → 缓存装载 → HIST-1 历史追赶 → 休眠至 target →
    HIST-2 缺口二次补齐 + WS 点火 → 收盘前 5s 点燃脉冲 REST → 全员收线放行（或 60s 硬熔断）
    → O(1) 切片交付 → 全量内存池丢后台落盘。
    出参形貌：{symbol: DataFrame[KLINE_COLS]}
    """
    t_start = time.time()
    log_prefix = f"[T-{uuid.uuid4().hex[:4].upper()}]"
    exchange = await _open_exchange(proxy_url, "INIT", log_prefix)

    try:
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)
        # logger.info(f"{log_prefix} [INIT] 🚀 K线极速引擎发车 | 目标=[{_format_bj_time(target_time_ms)}(+0800)] "
        #             f"周期=[{timeframe}] 天数=[{days}] 币种=[{len(symbol_list)}] 双擎=[WS:{use_ws} REST:{use_rest}]")

        memory_pool, fetch_since_map = load_local_cache(
            symbol_list, start_time_ms, timeframe_ms, timeframe, log_prefix=log_prefix)

        queue = asyncio.Queue()
        completion_event = asyncio.Event()
        processor_stats = {"reached": set(), "winners": {"WS": 0, "REST_POLL": 0, "HIST": 0},
                           "throughput": {}, "details": {}, "log_prefix": log_prefix}
        processor_task = asyncio.create_task(
            data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                           completion_event, memory_pool, processor_stats))

        # 阶段一：首波历史追赶（能否命中缓存决定这里的网络量级）
        hist_tracker_1 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0,
                          'latest_ts': 0, 'phase': 'HIST-1', 'log_prefix': log_prefix}
        history_tasks = [asyncio.create_task(
            fetch_historical_rest(exchange, sym, timeframe, fetch_since_map[sym], queue, hist_tracker_1))
            for sym in symbol_list]

        # 战术休眠一：睡到目标 K 线开盘，醒来立刻做缺口二次补齐，为收盘冲刺卸压
        sleep_to_target = target_time_ms - exchange.milliseconds()
        if sleep_to_target > 0:
            logger.info(f"{log_prefix} [SYNC] 💤 一阶段战术休眠（等目标K线开盘） | 睡眠=[{sleep_to_target / 1000:.1f}s] "
                        f"唤醒点=[{_format_bj_time(target_time_ms)}]")
            await asyncio.sleep(sleep_to_target / 1000)

        hist_tracker_2 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0,
                          'latest_ts': 0, 'phase': 'HIST-2', 'log_prefix': log_prefix}
        history_tasks.extend(asyncio.create_task(fetch_historical_rest(
            exchange, sym, timeframe,
            max(memory_pool[sym].keys()) if memory_pool[sym] else fetch_since_map[sym],
            queue, hist_tracker_2)) for sym in symbol_list)

        engine_tasks = []
        if use_ws:
            engine_tasks.append(asyncio.create_task(
                fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=log_prefix)))

        # 战术休眠二：死等到收盘前 5s，再瞬间点爆无延迟脉冲 REST
        sleep_to_rest = target_close_time_ms - 5000 - exchange.milliseconds()
        if sleep_to_rest > 0:
            # logger.info(f"{log_prefix} [SYNC] 💤 二阶段挂起（等收线冲刺窗口） | 睡眠=[{sleep_to_rest / 1000:.1f}s] "
            #             f"下一动作=[点燃REST脉冲轮询]")
            await asyncio.sleep(sleep_to_rest / 1000)

        if use_rest:
            engine_tasks.append(asyncio.create_task(
                fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue)))

        # 等待全员收线；超过「收盘 + 60s」硬熔断，宁可交付残缺也绝不挂死
        try:
            timeout = max(0.1, (target_close_time_ms + HARD_DEADLINE_MS - exchange.milliseconds()) / 1000)
            await asyncio.wait_for(completion_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            lag_symbols = sorted(set(symbol_list) - processor_stats["reached"])
            logger.warning(f"{log_prefix} [RACE] 🚨 触发绝对超时硬熔断（收盘后已等 60s），强制交卷 "
                           f"| 未到达=[{len(lag_symbols)}] 点名={lag_symbols[:5]}"
                           f"（可能原因：该币成交极度稀疏 / WS 与 REST 双通道均被网络阻断）")

        # 确保尽力而为的历史分页全部交卷，并把队列排空，让最后交付的缺口尽可能补齐
        await asyncio.gather(*history_tasks, return_exceptions=True)
        await queue.join()

        throughput = processor_stats.get('throughput', {})
        winners = processor_stats['winners']
        logger.info(f"{log_prefix} [RACE] 🎯 目标全线闭合 "
                    f"| 收线延迟=[{(exchange.milliseconds() - target_close_time_ms) / 1000:.3f}s] "
                    f"胜者=[WS:{winners['WS']} REST:{winners['REST_POLL']}] "
                    f"吞吐=[ws:{throughput.get('WS', 0)} rest:{throughput.get('REST_POLL', 0)} "
                    f"hist:{throughput.get('HIST', 0)}] 点名={processor_stats['details']}")

        # 发令枪响：强杀双擎与消费者（历史任务已自然跑完）
        all_tasks = engine_tasks + [processor_task]
        for task in all_tasks:
            if not task.done():
                task.cancel()
        await asyncio.gather(*all_tasks, return_exceptions=True)

        # 主线程只做 O(1) 切片，绝不碰全量排序（几十万行的脏活交给后台线程）
        final_dfs, gaps = {}, []
        expected_rows = int((target_time_ms - start_time_ms) / timeframe_ms) + 1
        for sym in symbol_list:
            klines = [k for ts, k in memory_pool[sym].items() if start_time_ms <= ts <= target_time_ms]
            klines.sort(key=lambda k: k[0])
            final_dfs[sym] = pd.DataFrame(klines, columns=KLINE_COLS)
            if len(klines) < expected_rows:
                gaps.append(f"{sym}(缺{expected_rows - len(klines)})")
        if gaps:
            logger.warning(f"{log_prefix} [CHECK] ⚠️ 交付数据存在断缺，请评估是否影响指标计算 "
                           f"| 涉及=[{len(gaps)}/{len(symbol_list)}] 应有行数=[{expected_rows}] 明细={gaps[:5]}"
                           f"（可能原因：该币上线较晚 / 交易所历史本身缺失 / 历史分页请求失败）")

        # 后台交接：全量清洗与落盘扔给子线程慢慢跑，主线程立刻返回
        dispatch_kline_background_save({sym: pool.copy() for sym, pool in memory_pool.items()},
                                       timeframe, cache_dir="data", log_prefix=log_prefix)

        logger.info(f"{log_prefix} [EXIT] 🎉 主任务零阻塞闪现交付 "
                    f"| 区间=[{_format_bj_time(start_time_ms)} ~ {_format_bj_time(target_time_ms)}] "
                    f"总行数=[{sum(len(df) for df in final_dfs.values())}] 全程耗时=[{time.time() - t_start:.2f}s]")
        return final_dfs
    finally:
        await _shutdown_exchange(exchange)


# =====================================================================
# 🗃️ 模块六：记录型缓存通用层（资金费率 / OI 共用）
# =====================================================================
def _load_records_cache(symbol_list, cache_dir, file_tail, columns, numeric_cols, tag, log_prefix=""):
    """
    通用「记录型」缓存装载：以 timestamp 为哈希 key，天然实现无缝时间轴去重。
    入参形貌：columns 首列必须为 'timestamp'；numeric_cols 中的列做 float 化（NaN → 0.0 防御填充）
    出参形貌：(memory_pool={symbol: {ts: {col: val}}}, max_ts_map={symbol: 最大缓存ts})
    """
    memory_pool = {sym: {} for sym in symbol_list}
    max_ts_map = {sym: 0 for sym in symbol_list}
    os.makedirs(cache_dir, exist_ok=True)
    hits, loaded_rows, broken = 0, 0, []

    for sym in symbol_list:
        path = _cache_path(cache_dir, sym, file_tail)
        if not os.path.exists(path):
            continue
        try:
            df = pd.read_csv(path)
            if df.empty or not set(columns).issubset(df.columns):
                continue
            for row in df[columns].values.tolist():
                ts = int(row[0])
                record = {'timestamp': ts}
                for col, val in zip(columns[1:], row[1:]):
                    record[col] = (float(val) if pd.notna(val) else 0.0) if col in numeric_cols else val
                memory_pool[sym][ts] = record
            max_ts_map[sym] = int(df['timestamp'].max())
            loaded_rows += len(df)
            hits += 1
        except Exception as e:
            broken.append(f"{sym}({e})")

    logger.info(
        f"{log_prefix} [{tag}_CACHE] ♻️ 缓存装载完毕 | 命中=[{hits}/{len(symbol_list)}] 载入行数=[{loaded_rows}]")
    if broken:
        logger.warning(f"{log_prefix} [{tag}_CACHE] ⚠️ 部分缓存读取失败，这些币将整段回补 "
                       f"| 数量=[{len(broken)}] 明细={broken[:3]}")
    return memory_pool, max_ts_map


def _dispatch_records_save(memory_pool_copy, cache_dir, file_tail, columns, tag, log_prefix=""):
    """
    记录型数据后台落盘（非守护线程，保证进程退出前写完）：ts 升序 → DataFrame(columns)
    → 临时文件 + 原子覆盖，避免半截文件损坏缓存。
    : 与 K 线不同，此处未使用跨进程读写锁；多进程同时落盘同一币种存在互相覆盖风险，待业务确认。
    """

    def _task():
        os.makedirs(cache_dir, exist_ok=True)
        saved, failed = 0, []
        for sym, records in memory_pool_copy.items():
            path = _cache_path(cache_dir, sym, file_tail)
            tmp_path = f"{path}.{uuid.uuid4().hex}.tmp"
            try:
                df = pd.DataFrame([records[ts] for ts in sorted(records)], columns=columns)
                df.to_csv(tmp_path, index=False)
                if _atomic_replace(tmp_path, path, log_prefix=log_prefix):
                    saved += 1
            except Exception as e:
                failed.append(f"{sym}({e})")
                if os.path.exists(tmp_path):
                    try:
                        os.remove(tmp_path)
                    except Exception:
                        pass
        logger.info(f"{log_prefix} [{tag}_DISK] 💾 后台落盘完毕 | 成功=[{saved}/{len(memory_pool_copy)}]")
        if failed:
            logger.error(f"{log_prefix} [{tag}_DISK] ❌ 部分币种落盘失败，下次运行需重新回补 | 明细={failed[:5]}")

    threading.Thread(target=_task, daemon=False).start()


def _deep_copy_records(memory_pool):
    """深拷贝记录型内存池，隔离后台落盘线程与主线程切片，避免共享 dict 被并发修改"""
    return {sym: {ts: dict(rec) for ts, rec in pool.items()} for sym, pool in memory_pool.items()}


def _slice_records(memory_pool, symbol_list, start_ms, end_ms, columns):
    """记录型内存池 → 闭区间 [start, end] 升序切片。出参：({symbol: DataFrame(columns)}, 总行数)"""
    dfs, total_rows = {}, 0
    for sym in symbol_list:
        records = [rec for ts, rec in memory_pool[sym].items() if start_ms <= ts <= end_ms]
        records.sort(key=lambda r: r['timestamp'])
        dfs[sym] = pd.DataFrame(records, columns=columns)
        total_rows += len(dfs[sym])
    return dfs, total_rows


# =====================================================================
# 💰 模块七：资金费率极速引擎
# =====================================================================
async def _fetch_funding_for_symbol(exchange, symbol, target_start_ms, memory_pool, max_cache_ts,
                                    threshold_ms=60000, log_prefix=""):
    """
    单币资金费率拉取，按「距下次结算的远近」双策略分流：
      A 结算抢跑：距结算 ≤ threshold_ms → 休眠到 T-5s，0.5s 脉冲轮询（最多 20 次）直到抓到新结算点；
      B 平稳回补：自「缓存最大 ts - 24h」重叠回补（无缓存则自 target_start_ms），分页拉到最新。
    ⚠️ 按原设计：本函数吞掉自身异常（仅打日志），避免单币故障拖垮整批 gather。
    出参：无返回，直接写 memory_pool[symbol][ts] = {'timestamp','fundingRate','symbol'}
    """
    try:
        try:
            funding_info = await _retry_async(lambda: exchange.fetch_funding_rate(symbol),
                                              f"[FUNDING] {symbol} 资金费率基础信息", log_prefix,
                                              attempts=3, delay=2.0)
        except Exception:
            logger.error(f"{log_prefix} [FUNDING_ERR] ❌ {symbol} 基础信息获取失败，本币本轮跳过 "
                         f"（可能原因：该合约已下线 / 网络长时不通）")
            return
        if not funding_info:
            return

        # 深度兼容：CCXT 对币安各版本字段映射不一致，逐层下挖直到原始 info
        next_funding_time = funding_info.get('nextFundingTimestamp') or funding_info.get('fundingTimestamp')
        if not next_funding_time:
            raw_next = (funding_info.get('info') or {}).get('nextFundingTime')
            next_funding_time = int(raw_next) if raw_next else None

        # 交易所未返回结算时间时 time_to_funding 取 -1，强行走场景 B 兜底
        time_to_funding = (next_funding_time - exchange.milliseconds()) if next_funding_time else -1

        # ---------- 场景 A：结算抢跑 ----------
        if 0 < time_to_funding <= threshold_ms:
            sleep_sec = max(0, (time_to_funding - 5000) / 1000.0)
            logger.info(f"{log_prefix} [FUNDING_SNIPE] 🎯 {symbol} 进入结算抢跑模式 "
                        f"| 距结算=[{time_to_funding / 1000:.1f}s] 休眠=[{sleep_sec:.1f}s] 之后每 0.5s 脉冲探测")
            await asyncio.sleep(sleep_sec)

            since_ms = next_funding_time - MS_PER_DAY  # 多取一日，防交易所只回吐一条旧记录
            for pulse in range(1, 21):  # 保护性熔断，绝不允许死循环
                rates = None
                try:
                    rates = await exchange.fetch_funding_rate_history(symbol, since=since_ms, limit=100)
                except Exception as e:
                    logger.warning(f"{log_prefix} [FUNDING_SNIPE] ⚠️ {symbol} 脉冲抓取异常，稍后重试 "
                                   f"| 第=[{pulse}/20]次 错误=[{e}]")
                if rates:
                    for r in rates:
                        ts = r['timestamp']
                        memory_pool[symbol][ts] = {'timestamp': ts, 'fundingRate': r['fundingRate'], 'symbol': symbol}
                    newest_ts = max(r['timestamp'] for r in rates)
                    if newest_ts >= next_funding_time:
                        logger.info(f"{log_prefix} [FUNDING_SNIPE] 🔫 {symbol} 脉冲命中最新结算点 "
                                    f"| 结算时间=[{_format_bj_time(newest_ts)}] 脉冲次数=[{pulse}]")
                        return
                await asyncio.sleep(0.5)
            logger.warning(f"{log_prefix} [FUNDING_SNIPE] ⚠️ {symbol} 脉冲 20 次仍未等到新结算点，本次交付已有历史 "
                           f"（可能原因：交易所结算数据发布延迟）")
            return

        # ---------- 场景 B：平稳期重叠回补 ----------
        since_ms = max_cache_ts - MS_PER_DAY if max_cache_ts > 0 else target_start_ms
        next_time_str = _format_bj_time(next_funding_time) if next_funding_time else '未知'
        dist_str = f"{time_to_funding / 1000:.1f}s" if time_to_funding > 0 else '未知'
        logger.info(f"{log_prefix} [FUNDING_REST] 🛒 {symbol} 距结算较远，执行常规回补 "
                    f"| 下次结算=[{next_time_str}] 距今=[{dist_str}] 阈值=[{threshold_ms / 1000}s] "
                    f"回补起点=[{_format_bj_time(since_ms)}]")

        curr_since = since_ms
        while True:
            hist_rates = await _retry_async(
                lambda: exchange.fetch_funding_rate_history(symbol, since=curr_since, limit=1000),
                f"[FUNDING] {symbol} 历史分页(since={_format_bj_time(curr_since)})", log_prefix,
                attempts=3, delay=2.0)
            if not hist_rates:
                break
            for r in hist_rates:
                ts = r['timestamp']
                memory_pool[symbol][ts] = {'timestamp': ts, 'fundingRate': r['fundingRate'], 'symbol': symbol}
            if len(hist_rates) < 1000:
                break
            curr_since = hist_rates[-1]['timestamp'] + 1

    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"{log_prefix} [FUNDING_ERR] ❌ {symbol} 资金费率拉取异常退出，该币仅交付已有缓存 "
                     f"| 错误=[{e}]（可能原因：网络长时中断 / 交易所返回结构异常）")


async def _async_core_funding_orchestrator(symbol_list, days, proxy_url):
    """
    资金费率编排。数据流：缓存装载 → 逐币并发拉取（抢跑/回补分流）→ 深拷贝丢后台落盘 → O(1) 切片交付。
    : 右边界固定取「交易所当前时间」，刻意忽略调用方的时间语义，保留原行为。
    出参形貌：{symbol: DataFrame[FUNDING_COLS]}
    """
    t_start = time.time()
    log_prefix = f"[F-{uuid.uuid4().hex[:4].upper()}]"
    exchange = await _open_exchange(proxy_url, "FUNDING_INIT", log_prefix)

    try:
        target_end_ms = exchange.milliseconds()
        target_start_ms = target_end_ms - int(float(days) * MS_PER_DAY)
        logger.info(f"{log_prefix} [FUNDING_INIT] 🚀 资金费率极速引擎发车 "
                    f"| 区间=[{_format_bj_time(target_start_ms)} ~ {_format_bj_time(target_end_ms)}] "
                    f"天数=[{days}] 币种=[{len(symbol_list)}]")

        memory_pool, max_cache_ts_map = _load_records_cache(
            symbol_list, "data", "funding_latest.csv", FUNDING_COLS, {'fundingRate'}, "FUNDING", log_prefix)

        # 单币异常已在协程内部消化，此处无需 return_exceptions
        await asyncio.gather(*[
            _fetch_funding_for_symbol(exchange, sym, target_start_ms, memory_pool,
                                      max_cache_ts_map[sym], 60000, log_prefix)
            for sym in symbol_list])

        _dispatch_records_save(_deep_copy_records(memory_pool), "data", "funding_latest.csv",
                               FUNDING_COLS, "FUNDING", log_prefix)

        final_dfs, total_rows = _slice_records(memory_pool, symbol_list, target_start_ms, target_end_ms, FUNDING_COLS)
        logger.info(f"{log_prefix} [FUNDING_EXIT] 🎉 资金费率交付完毕 "
                    f"| 总行数=[{total_rows}] 币种=[{len(final_dfs)}] 全程耗时=[{time.time() - t_start:.2f}s]")
        return final_dfs
    finally:
        await _shutdown_exchange(exchange)


# =====================================================================
# 📊 模块八：OI 未平仓量极速引擎（纯 REST 轮询，双列极简）
# =====================================================================
async def _fetch_oi_for_symbol(exchange, symbol, timeframe, start_ms, must_reach_ms,
                               memory_pool, max_cache_ts, log_prefix=""):
    """
    单币 OI 拉取：纯 REST 分页轮询，直到数据覆盖 must_reach_ms 为止。
    历史追赶 → 目标未产出则战术休眠 → 目标已过但交易所未刷新则 1s 脉冲轮询。
    死循环的唯一护栏是编排层的绝对超时 cancel（本函数不自行设上限）。
    ⚠️ 按原设计吞掉自身异常，保全整批任务；出参直接写 memory_pool[symbol][ts]={'timestamp','oi_amount'}
    """
    try:
        timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
        # 有缓存则回退 2 根做重叠拼接，防边界数据被截断
        curr_since = max_cache_ts - timeframe_ms * 2 if max_cache_ts > 0 else start_ms
        logger.info(f"{log_prefix} [OI_REST] 🛒 {symbol} 启动 REST 分页轮询 "
                    f"| 起点=[{_format_bj_time(curr_since)}] 必须覆盖=[{_format_bj_time(must_reach_ms)}]")

        while True:
            hist_oi = await _retry_async(
                lambda: exchange.fetch_open_interest_history(symbol, timeframe, since=curr_since, limit=500),
                f"[OI] {symbol} OI 历史分页(since={_format_bj_time(curr_since)})", log_prefix,
                attempts=3, delay=2.0)

            if hist_oi:
                for r in hist_oi:
                    ts = int(r['timestamp'])
                    amount = r.get('openInterestAmount') or 0.0
                    if not amount:  # 标准字段缺失时下挖原始 info（币本位持仓量）
                        raw_amount = (r.get('info') or {}).get('sumOpenInterest')
                        amount = float(raw_amount) if raw_amount else 0.0
                    memory_pool[symbol][ts] = {'timestamp': ts, 'oi_amount': float(amount)}

                latest_ts = int(hist_oi[-1]['timestamp'])
                curr_since = latest_ts + 1
                if latest_ts >= must_reach_ms:
                    logger.info(f"{log_prefix} [OI_REST] 🎯 {symbol} 已覆盖目标边界，收工 "
                                f"| 最新=[{_format_bj_time(latest_ts)}] 内存池行数=[{len(memory_pool[symbol])}]")
                    return
                if len(hist_oi) == 500:
                    continue  # 满页说明历史还没拉完，直接翻下一页
            elif memory_pool[symbol] and max(memory_pool[symbol]) >= must_reach_ms:
                return  # 兜底：空返回但水位已达标（防死循环）

            # 历史已追到当前最新，但仍未触达目标 → 挂起等待目标数据产出
            wait_ms = must_reach_ms - time.time() * 1000
            if wait_ms > 0:
                sleep_sec = wait_ms / 1000.0 + 1.0  # +1s 防提前苏醒空跑
                logger.info(f"{log_prefix} [OI_REST] 💤 {symbol} 目标 K 线尚未产出，休眠后冲刺 | 睡眠=[{sleep_sec:.1f}s]")
                await asyncio.sleep(sleep_sec)
            else:
                await asyncio.sleep(1.0)  # 目标时间已过但交易所还没刷出，1s 脉冲兼顾速度与防封
    except asyncio.CancelledError:
        raise  # 响应编排层的超时熔断
    except Exception as e:
        logger.error(f"{log_prefix} [OI_ERR] ❌ {symbol} OI 拉取异常退出，该币仅交付已有缓存 "
                     f"| 错误=[{e}]（可能原因：交易所 OI 接口不支持该合约 / 网络长时中断）")


async def _async_core_oi_orchestrator(symbol_list, timeframe, days, target_time_str, proxy_url):
    """
    OI 编排。链路：建连 → 时间对齐 → 缓存装载 → 逐币并发 REST 轮询（含绝对超时熔断）
    → 深拷贝丢后台落盘 → 切片交付两列极简数据。
    : 拉取阶段要求覆盖 target_close_time_ms（target + 1 周期），但最终切片只到 target_time_ms；
           即多等一个桶仅用于确认目标那根已定型，保留原有潜规则不做变更。
    出参形貌：{symbol: DataFrame[OI_COLS]}
    """
    t_start = time.time()
    log_prefix = f"[O-{uuid.uuid4().hex[:4].upper()}]"
    exchange = await _open_exchange(proxy_url, "OI_INIT", log_prefix)

    try:
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)
        logger.info(f"{log_prefix} [OI_INIT] 🚀 OI 未平仓极速引擎发车 | 目标=[{_format_bj_time(target_time_ms)}] "
                    f"周期=[{timeframe}] 天数=[{days}] 币种=[{len(symbol_list)}]")

        memory_pool, max_cache_ts_map = _load_records_cache(
            symbol_list, "data", f"oi_{timeframe}_latest.csv", OI_COLS, {'oi_amount'}, "OI", log_prefix)

        tasks = [asyncio.create_task(
            _fetch_oi_for_symbol(exchange, sym, timeframe, start_time_ms, target_close_time_ms,
                                 memory_pool, max_cache_ts_map[sym], log_prefix))
            for sym in symbol_list]

        # 绝对超时：与 K 线引擎对齐（目标收盘 + 60s），防 API 假死导致永久挂起
        timeout = max(5.0, (target_close_time_ms + HARD_DEADLINE_MS - exchange.milliseconds()) / 1000.0)
        try:
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"{log_prefix} [OI_RACE] 🚨 触发绝对超时硬熔断（收盘后已等 60s），强制终止轮询并交卷 "
                           f"| 超时阈值=[{timeout:.1f}s]（可能原因：交易所 OI 数据发布延迟 / 网络阻断）")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        _dispatch_records_save(_deep_copy_records(memory_pool), "data", f"oi_{timeframe}_latest.csv",
                               OI_COLS, "OI", log_prefix)

        final_dfs, total_rows = _slice_records(memory_pool, symbol_list, start_time_ms, target_time_ms, OI_COLS)
        logger.info(f"{log_prefix} [OI_EXIT] 🎉 OI 纯REST交付完毕 "
                    f"| 区间=[{_format_bj_time(start_time_ms)} ~ {_format_bj_time(target_time_ms)}] "
                    f"总行数=[{total_rows}] 全程耗时=[{time.time() - t_start:.2f}s]")
        return final_dfs
    finally:
        await _shutdown_exchange(exchange)


# =====================================================================
# 🌟 对外暴露的公共 API
# =====================================================================
def snipe_kline_data(symbol_list, timeframe, days, target_time_str,
                     use_ws=True, use_rest=True, proxy_url=None,
                     dedupe=True,
                     cache_dir="data",
                     lock_timeout=None,
                     snapshot_ttl_sec=None,
                     incomplete_snapshot_ttl_sec=600,
                     snapshot_gc_keep_sec=86400):
    """
    🚀 同步入口：极速狙击指定时间的 K 线数据。

    【跨进程单飞去重 Single-Flight】同机多进程同参请求时，只有一个进程真实打网络（Leader），
    其余进程复用其结果（Follower），把网络权重与 IO 压力从 N 降到 1。

    :param dedupe: 是否启用跨进程去重（False = 100% 退回旧版行为，可用于灰度 /紧急回滚）
    :param cache_dir: 缓存根目录，快照落在 {cache_dir}/_snapshots，锁落在 {cache_dir}/_locks
    :param lock_timeout: 排队等锁上限秒数。None = 自动按「目标收盘 + 60s 硬熔断 + 历史补偿 + 180s 余量」推算
    :param snapshot_ttl_sec: 完整快照有效期（None = 永久，历史 K 线是幂等的）
    :param incomplete_snapshot_ttl_sec: 「有断缺」快照的宽限复用期，用于吸收瞬时并发风暴
    :param snapshot_gc_keep_sec: 快照磁盘保留时长，超期由后台 GC 清理
    :return: {symbol: DataFrame[timestamp, open, high, low, close, volume]}（key 与传入 symbol_list 完全一致）
    """
    _ensure_no_running_loop()

    def _run_core():
        return asyncio.run(_async_core_sniping_orchestrator(
            symbol_list, timeframe, days, target_time_str, use_ws, use_rest, proxy_url))

    if not symbol_list:
        return {}
    if not dedupe:
        return _run_core()

    # ---------- 1. 构建请求指纹（去重是加速手段，任何失败都必须静默降级，绝不拖垮业务） ----------
    try:
        sig = _build_kline_request_signature(symbol_list, timeframe, days, target_time_str)
    except Exception as e:
        logger.warning(f"[DEDUPE] ⚠️ 请求指纹构建失败，本次退化为独立拉取 | 错误=[{e}]")
        return _run_core()

    snapshot_dir = os.path.join(cache_dir, _SNAPSHOT_DIRNAME)
    lock_dir = os.path.join(cache_dir, _LOCK_DIRNAME)
    try:
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(lock_dir, exist_ok=True)
    except Exception as e:
        logger.warning(f"[DEDUPE] ⚠️ 去重目录创建失败，本次退化为独立拉取 | 错误=[{e}]")
        return _run_core()

    log_prefix = f"[DEDUPE|{sig['key'][-17:]}|PID{os.getpid()}]"

    def _remap(payload):
        """快照按归一化 symbol 存储，返回时映射回调用方原始 key，保证外部完全无感"""
        if payload is None:
            return None
        try:
            return {sym: payload[str(sym).strip()] for sym in symbol_list}
        except KeyError:
            return None

    # ---------- 2. L1 无锁快路径：快照直出，零网络请求 ----------
    hit = _remap(_read_kline_snapshot(sig, snapshot_dir, snapshot_ttl_sec, incomplete_snapshot_ttl_sec, log_prefix))
    if hit is not None:
        logger.info(
            f"{log_prefix} ⚡ L1 快照直出 | 币种=[{len(hit)}] 行数=[{sum(len(v) for v in hit.values())}] 网络请求=[0]")
        return hit

    # ---------- 3. L2 抢跨进程锁 + 双重检查锁定 ----------
    if lock_timeout is None:
        base_wait = (sig['target_close_time_ms'] + HARD_DEADLINE_MS - time.time() * 1000) / 1000.0
        history_buffer = float(sig['days']) * 2.0  # 每回溯 1 天多给 2s 网络 IO 补偿
        lock_timeout = max(180.0, base_wait + history_buffer + 180.0)

    mutex = InterProcessMutex(os.path.join(lock_dir, f"{sig['key']}.lock"))
    wait_t0 = time.time()

    def _on_wait(waited):
        # logger.info(f"{log_prefix} ⏳ 同参进程正在拉取，本进程排队等待中 "
        #             f"| 已等=[{waited:.0f}s] 上限=[{lock_timeout:.0f}s]")
        pass

    if not mutex.acquire(timeout=lock_timeout, poll_interval=0.2, on_wait=_on_wait, wait_log_interval=45.0):
        logger.warning(f"{log_prefix} ⚠️ 等锁超时，为保障可用性降级为独立拉取（会产生重复网络请求） "
                       f"| 上限=[{lock_timeout:.0f}s]（可能原因：Leader 进程卡死 / 单次拉取耗时远超预期）")
        return _run_core()

    try:
        hit = _remap(_read_kline_snapshot(sig, snapshot_dir, snapshot_ttl_sec, incomplete_snapshot_ttl_sec, log_prefix))
        if hit is not None:
            # logger.info(f"{log_prefix} ✅ L2 双重检查命中，直接复用 Leader 成果 "
            #             f"| 等锁=[{time.time() - wait_t0:.2f}s] 行数=[{sum(len(v) for v in hit.values())}] 网络请求=[0]")
            return hit

        # logger.info(f"{log_prefix} 👑 当选 Leader，开始真实拉取 | 等锁=[{time.time() - wait_t0:.2f}s] "
        #             f"目标=[{_format_bj_time(sig['target_time_ms'])}] 币种=[{len(sig['symbols'])}]")
        result = _run_core()

        # 交付前必须同步写快照：Follower 是在本进程释放锁之后才做双重检查的
        try:
            _write_kline_snapshot(sig, result, snapshot_dir, log_prefix)
        except Exception as e:
            logger.error(f"{log_prefix} ❌ 快照写入异常，其它进程将各自拉取（不影响本次返回） | 错误=[{e}]")
        return result
    finally:
        try:
            mutex.release()
        except Exception as e:
            logger.warning(f"{log_prefix} ⚠️ 锁释放异常（操作系统会在进程退出时自动回收） | 错误=[{e}]")
        _maybe_dispatch_dedupe_gc(snapshot_dir, lock_dir, snapshot_gc_keep_sec, log_prefix)


def snipe_funding_rate_data(symbol_list, days, proxy_url=None):
    """
    🚀 同步入口：获取资金费率（本地缓存 + 临近结算脉冲抢跑 + 时间轴哈希去重）。
    :param days: 回溯天数（右边界为交易所当前时间）
    :return: {symbol: DataFrame[timestamp, fundingRate, symbol]}
    """
    _ensure_no_running_loop()
    return asyncio.run(_async_core_funding_orchestrator(symbol_list, days, proxy_url))


def snipe_oi_data(symbol_list, timeframe, days, target_time_str, proxy_url=None):
    """
    🚀 同步入口：获取 Open Interest（未平仓合约）历史数据。纯 REST 全链路轮询，
    死等目标时间的数据产出，返回 [timestamp, oi_amount] 双列极简切片。
    :return: {symbol: DataFrame[timestamp, oi_amount]}
    """
    _ensure_no_running_loop()
    return asyncio.run(_async_core_oi_orchestrator(symbol_list, timeframe, days, target_time_str, proxy_url))


# =====================================================================
# ➕ 模块九：新增融合功能区 (历史1h K线更新与实时信号组装)
# =====================================================================
DATA_DIR_V2 = './data/history'
REQUIRED_KLINE_COUNT = 70 * 24


def get_symbol_filename(symbol):
    """辅助函数：将 ccxt 的币种名(如 BTC/USDT:USDT) 转为文件名 BTC_USDT_1h_history.csv"""
    base_name = symbol.split(':')[0].replace('/', '_')
    return f"{base_name}_1h_history.csv"




# =====================================================================
# 🚀 模块十：历史更新与收盘信号极速聚合 (新增融合模块)
# =====================================================================

async def _async_snipe_and_update_hourly_signals(target_timestamp_ms, proxy_url, required_days=70):
    """
    异步核心：卡点轮询收线 -> 增量更新历史 -> 严格校验连续性 -> 返回合格信号切片
    """
    log_prefix = "[SNIPE_SIGNAL]"
    logger.info(f"{log_prefix} 🚀 启动一站式数据聚合引擎...")

    exchange = await _open_exchange(proxy_url, "SNIPE_SIGNAL", log_prefix)
    try:
        # 1. 时间边界计算：收缩到最近的整点小时
        current_hour_ms = target_timestamp_ms - (target_timestamp_ms % (3600 * 1000))
        next_hour_ms = current_hour_ms + (3600 * 1000)
        wait_target_ms = next_hour_ms - 5000  # 提前 5s 进入轮询状态

        required_kline_count = required_days * 24
        # 多冗余拉取几天防止断层
        max_history_ms = (required_days + 2) * 24 * 3600 * 1000

        if not os.path.exists(DATA_DIR_V2):
            os.makedirs(DATA_DIR_V2)

        # 2. 战术休眠：如果时间还没到 提前5s 的阈值，先睡过去
        now_ms = int(time.time() * 1000)
        if now_ms < wait_target_ms:
            sleep_sec = (wait_target_ms - now_ms) / 1000.0
            logger.info(
                f"{log_prefix} 💤 未到收线冲刺时间，休眠 [{sleep_sec:.1f}s] 至 [{_format_bj_time(wait_target_ms)}]")
            await asyncio.sleep(sleep_sec)

        # 3. 脉冲轮询：盯盘 BTC/USDT 确认本小时彻底收线
        logger.info(
            f"{log_prefix} 🎯 进入极速轮询阶段，盯盘 BTC/USDT 确认 [{_format_bj_time(current_hour_ms)}] K线收线...")
        while True:
            now_ms = int(time.time() * 1000)
            try:
                # 请求 limit=2 确保能看到 next_hour_ms 的新K线冒出来
                klines = await _retry_async(
                    lambda: exchange.fetch_ohlcv('BTC/USDT:USDT', '1h', limit=2),
                    "轮询 BTC/USDT 收线状态", log_prefix, attempts=1, delay=0.5
                )
                if klines and klines[-1][0] >= next_hour_ms:
                    logger.info(
                        f"{log_prefix} 🏁 观测到新小时 K 线产生，确认全市场 [{_format_bj_time(current_hour_ms)}] 已闭合！")
                    break
            except Exception:
                pass  # 吞掉单次轮询异常，靠循环自愈

            # 硬熔断保护：最多死等 60s
            if now_ms > next_hour_ms + 60000:
                logger.warning(f"{log_prefix} ⚠️ 轮询收线超时 60s，强制放行执行后续更新。")
                break
            await asyncio.sleep(0.5)

        # 4. 获取符合条件的标的 (重新 load_markets 确保拿到最新上下币状态)
        await _retry_async(lambda: exchange.load_markets(True), "刷新市场状态", log_prefix, attempts=3, delay=1.0)
        symbols = [
            s for s in exchange.symbols
            if exchange.market(s).get('linear')
               and exchange.market(s).get('active')
               and exchange.market(s).get('info', {}).get('status') == 'TRADING'
               and s.endswith(':USDT')
        ]
        logger.info(f"{log_prefix} 🔍 全市场扫尾，发现 [{len(symbols)}] 个活跃 USDT 本位合约")

        # 5. 并发拉取更新与组装核对
        updated_count = 0
        skipped_count = 0
        valid_data_dict = {}
        invalid_symbols = []

        for symbol in symbols:
            file_name = get_symbol_filename(symbol)
            file_path = os.path.join(DATA_DIR_V2, file_name)

            df_local = pd.DataFrame()
            start_fetch_ms = current_hour_ms - max_history_ms
            need_fetch = True

            # -- A. 读取本地缓存决定拉取策略 --
            if os.path.exists(file_path):
                try:
                    df_local = pd.read_csv(file_path)
                    if not df_local.empty and 'timestamp' in df_local.columns:
                        local_last_ms = int(df_local['timestamp'].iloc[-1])
                        if local_last_ms >= current_hour_ms:
                            need_fetch = False
                            skipped_count += 1
                        else:
                            start_fetch_ms = max(local_last_ms - (24 * 3600 * 1000), current_hour_ms - max_history_ms)
                except Exception as e:
                    logger.warning(f"{log_prefix} ⚠️ {symbol} 缓存损坏，触发全量回拉: {e}")

            df_combined = df_local

            # -- B. 增量拉取与合并 --
            if need_fetch:
                all_new_klines = []
                curr_start = start_fetch_ms

                try:
                    while curr_start <= current_hour_ms:
                        klines = await _retry_async(
                            lambda: exchange.fetch_ohlcv(symbol, '1h', since=curr_start, limit=1500),
                            f"拉取 {symbol} 历史", log_prefix, attempts=3, delay=1.0
                        )
                        if not klines:
                            break
                        valid_klines = [k for k in klines if k[0] <= current_hour_ms]
                        all_new_klines.extend(valid_klines)

                        if len(klines) < 1500:
                            break
                        curr_start = klines[-1][0] + (3600 * 1000)
                        await asyncio.sleep(0.05)
                except Exception as e:
                    logger.error(f"{log_prefix} ❌ {symbol} 数据拉取中断: {e}")
                    continue

                if all_new_klines:
                    df_new = pd.DataFrame(all_new_klines, columns=KLINE_COLS)
                    df_combined = pd.concat([df_local, df_new], ignore_index=True) if not df_local.empty else df_new

                    df_combined = df_combined.drop_duplicates(subset=['timestamp'], keep='last')
                    df_combined = df_combined.sort_values('timestamp').reset_index(drop=True)
                    df_combined['timestamp'] = df_combined['timestamp'].astype('int64')

                    # 磁盘写入
                    df_combined.tail(required_kline_count + 24).to_csv(file_path, index=False)
                    updated_count += 1

            # -- C. 严格校验器 (Data Validator) --
            if df_combined.empty:
                invalid_symbols.append((symbol, "数据最终为空"))
                continue

            df_combined['timestamp'] = df_combined['timestamp'].astype('int64')
            df_check = df_combined.set_index('timestamp').sort_index()

            # 裁剪掉超越了 current_hour_ms 的未来冗余数据（如果缓存里本来就有）
            df_check = df_check[df_check.index <= current_hour_ms]

            if len(df_check) < required_kline_count:
                invalid_symbols.append(
                    (symbol, f"K线数量不足 (仅有 {len(df_check)} 根，需要 {required_kline_count} 根)"))
                continue

            # 截取最后的 required_kline_count 行 (70天)
            tail_df = df_check.tail(required_kline_count)

            if tail_df.index[-1] != current_hour_ms:
                invalid_symbols.append(
                    (symbol, f"未能对齐目标收盘整点 (最后数据时间为 {_format_bj_time(tail_df.index[-1])})"))
                continue

            time_diffs = tail_df.index.to_series().diff().dropna()
            if not (time_diffs == 3600000).all():
                invalid_symbols.append((symbol, "1小时K线内部存在断层/间断"))
                continue

            # 测试合格，归档入库
            valid_data_dict[symbol] = tail_df.reset_index()

        # 6. 归档总结
        logger.info(f"\n=================== 信号交付报告 ===================")
        logger.info(f"✅ 合格并发放信号币种: {len(valid_data_dict)} 个")
        logger.info(f"🔄 IO统计: 拉取更新 {updated_count} 个 | 命中缓存跳过 {skipped_count} 个")
        if invalid_symbols:
            logger.info(f"❌ 淘汰剔除币种: {len(invalid_symbols)} 个")
            # 出于控制台清爽，最多打印前 5 个淘汰原因
            for sym, reason in invalid_symbols[:5]:
                logger.info(f"   - {sym}: {reason}")
            if len(invalid_symbols) > 5:
                logger.info(f"   - ... (及其他 {len(invalid_symbols) - 5} 个)")
        logger.info(f"====================================================\n")

        return valid_data_dict
    finally:
        await _shutdown_exchange(exchange)


def snipe_and_update_hourly_signals(target_timestamp_ms, proxy=None, required_days=70):
    """
    同步接口：历史数据拉取、收线卡点等待、以及数据核验组装的终极融合方法。

    :param target_timestamp_ms: 目标时间戳(毫秒)，函数内部会自动将其向下取整为最近的小时边界。
    :param proxy: 代理地址 (e.g., 'http://127.0.0.1:7890')
    :param required_days: 需要连续不断的天数，默认为 70 天。
    :return: dict 格式 {symbol: DataFrame(含timestamp等6列)}，只返回彻底合格的数据。
    """
    _ensure_no_running_loop()
    return asyncio.run(_async_snipe_and_update_hourly_signals(target_timestamp_ms, proxy, required_days))

# =====================================================================
# 🚀 启动入口（已完美融通保留旧有功能与新整合功能的示例代码）
# =====================================================================
if __name__ == "__main__":
    # # --- 原极速底座示例保留 ---
    # symbol_list = ["BTC/USDC:USDC"]
    # target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")
    #
    # logger.info(f">>> 准备调用 OI 未平仓合约极速引擎 | 币种={symbol_list} 目标时间=[{target_time}]")
    # oi_result_map = snipe_oi_data(
    #     symbol_list=symbol_list,
    #     timeframe="5m",
    #     days=20,
    #     target_time_str=target_time,
    #     proxy_url='http://127.0.0.1:7890'
    # )
    # logger.info(f"✅ OI 数据已交付，自动合并落盘并返回两列 O(1) 切片 "
    #             f"| 币种=[{len(oi_result_map)}] 总行数=[{sum(len(df) for df in oi_result_map.values())}]")

    # ==========================================
    # 🚀 新增融合功能演示 (实盘骨架)
    # ==========================================
    now_ms = int((time.time() - 60 * 60) * 1000)
    valid_signals = snipe_and_update_hourly_signals(
        target_timestamp_ms=now_ms,
        proxy='http://127.0.0.1:7890',
        required_days=70
    )
    pass