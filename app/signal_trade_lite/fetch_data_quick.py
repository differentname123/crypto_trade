# -*- coding: utf-8 -*-
"""
:description:
基于生产者-消费者模型的高并发、极速、双引擎 K 线数据获取基建。
【升级版】增加本地缓存智能读取、独立守护线程无阻塞落盘、模块化重构。
【性能修复版】彻底消除 iterrows 主线程阻塞，重构后台落盘规避冗余 IO 与数据丢失。
【终极优化版】修复主线程切片双重计算冗余、引入临时文件原子性落盘防损坏、静默回收取消协程防止内存泄露。
【极限竞速版】二次历史前置释放、REST 脏数据严格过滤、最后 5s 无延迟脉冲式狂暴轮询。
【生产监控版】引入 TraceID 链路追踪、Logfmt 结构化高密度聚合、静默成功与 O(N) 滞后点名机制。
【防弹修复版】消除 WS 硬编码兼容全币种，注入协程防异常死锁装甲，引入物理时钟强判防流动性枯竭。
【闪现交付版】主线程金蝉脱壳 O(1) 返回、剔除 datetime_bj 性能枷锁、后台线程接管全量清洗落盘。
【资金费率极速扩展】新增无缝时间轴哈希去重、阈值脉冲探测结算抢跑、缓存回退拼接兜底方案。
【全链路防断网修复版】为所有核心 HTTP/CCXT 请求覆盖指数退避与防弹装甲，抵御 WinError 64 闪断异常。
【OI未平仓合约极速扩展版】新增 Open Interest 历史持仓量全链路极速拉取引擎（纯REST轮询，双列极简版）。
"""

import asyncio
import uuid
import ccxt.async_support as ccxt
import pandas as pd
import time
import os
import json
import logging
import aiohttp
import threading
from datetime import datetime, timedelta

from common_utils_lite import setup_logger
import hashlib
import pickle
import re
import glob
# 解除 Pandas 控制台打印限制
pd.set_option('display.max_columns', None)
pd.set_option('display.width', 1000)

# 配置基础日志
logger = setup_logger()


def _format_bj_time(ts_ms):
    """辅助函数：将时间戳统一格式化为北京时间字符串，消除时区歧义"""
    return pd.to_datetime(ts_ms, unit='ms').tz_localize('UTC').tz_convert('Asia/Shanghai').strftime('%Y-%m-%d %H:%M:%S')

# =====================================================================
# 🔐 模块零：跨进程去重基建 (Inter-Process Dedupe Infrastructure)
# ---------------------------------------------------------------------
# 目标：同一台机器上、多进程发起「完全同参」的 K 线请求时，
#      只允许一个进程真实打网络（Leader），其余进程等待并直接复用其结果（Follower）。
# 结构：L1 无锁快照直出 → L2 文件锁 + 双重检查锁定 → L3 等锁超时降级自取（永不失败）
# =====================================================================

_SNAPSHOT_VERSION = 2
_SNAPSHOT_DIRNAME = "_snapshots"
_LOCK_DIRNAME = "_locks"
_GC_MIN_INTERVAL_SEC = 600  # GC 节流：同一目录最多 10 分钟扫一次

# ------------------------ 底层文件锁后端 ------------------------
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
    → 进程崩溃/被强杀时由操作系统自动回收，不存在"死锁文件"残留问题。
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


import weakref


class _LockWrapper:
    """包装原生的 threading.Lock，使其支持弱引用 (WeakValueDictionary)"""

    def __init__(self):
        self.lock = threading.Lock()

    def acquire(self, *args, **kwargs):
        return self.lock.acquire(*args, **kwargs)

    def release(self):
        return self.lock.release()


# 将普通的 dict 替换为弱引用字典，彻底解决长生命周期进程的内存泄漏
_THREAD_LOCKS = weakref.WeakValueDictionary()
_THREAD_LOCKS_GUARD = threading.Lock()


def _get_thread_lock(abs_path):
    """同一进程内、同一把文件锁路径共享一个线程锁，规避 flock 同进程双 FD 语义坑"""
    with _THREAD_LOCKS_GUARD:
        lk = _THREAD_LOCKS.get(abs_path)
        if lk is None:
            lk = _LockWrapper()  # 使用包装类，使其能被 GC 自动回收
            _THREAD_LOCKS[abs_path] = lk
        return lk


class InterProcessMutex:
    """
    跨进程 + 跨线程双层互斥锁。
    - acquire() 永不抛超时异常，只返回 True/False，交由调用方决定降级策略
    - 支持排队心跳回调 on_wait，避免长时间等待时日志"假死"
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
            ok = self._tlock.acquire(True)
        else:
            ok = self._tlock.acquire(True, max(0.0, deadline - time.monotonic()))
        if not ok:
            return False
        self._tlock_held = True

        # 2) 进程级互斥（轮询式非阻塞抢占，便于打心跳 & 精准控制超时）
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
            logger.warning(f"[MUTEX] ⚠️ 进程锁获取异常({e})，视为未取得锁")
            self._release_thread_lock()
            return False

    def _release_thread_lock(self):
        if self._tlock_held:
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


# ------------------------ 原子落盘 / 受保护读盘 ------------------------
def _atomic_replace(src, dst, retries=12, delay=0.15, log_prefix=""):
    """
    带退避重试的原子覆盖。
    Windows 下若目标文件正被其它进程 open 读取，os.replace 会抛 PermissionError，
    此处重试兜底，彻底失败则清理临时文件并返回 False（不影响主流程返回数据）。
    """
    last_err = None
    for _ in range(max(1, retries)):
        try:
            os.replace(src, dst)
            return True
        except Exception as e:
            last_err = e
            time.sleep(delay)
    logger.error(f"{log_prefix} [IO] ❌ 原子覆盖失败 -> {dst} | err={last_err}")
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
    加同一把读写锁读取 CSV，与后台落盘线程严格串行，规避 Windows replace 冲突与半截文件。
    抢不到锁时依然尽力读取（best-effort），绝不阻塞主链路。
    ⚠️ 注意：本函数内部会抢锁，禁止在已持有同一把锁的代码块中调用（线程锁不可重入）。
    """
    mutex = InterProcessMutex(_csv_rw_lock_path(path))
    got = mutex.acquire(timeout=timeout)
    try:
        last_err = None
        for attempt in range(3):
            try:
                return pd.read_csv(path)
            except Exception as e:
                last_err = e
                time.sleep(0.1)
        raise last_err
    finally:
        if got:
            mutex.release()


# ------------------------ 请求指纹 ------------------------
_TF_UNIT_MS = {'s': 1000, 'm': 60_000, 'h': 3_600_000, 'd': 86_400_000, 'w': 604_800_000}


def _parse_timeframe_ms(timeframe):
    """本地解析周期毫秒（不依赖 ccxt 实例，避免为了算指纹去建连交易所）"""
    m = re.fullmatch(r'\s*(\d+)\s*([smhdwM])\s*', str(timeframe))
    if not m:
        raise ValueError(f"无法解析的 timeframe: {timeframe}")
    num, unit = int(m.group(1)), m.group(2)
    if unit == 'M':
        raise ValueError("月线(M)无固定毫秒长度，不支持指纹级对齐")
    return num * _TF_UNIT_MS[unit]


def _build_kline_request_signature(symbol_list, timeframe, days, target_time_str):
    """
    构建请求指纹。必须与 parse_time_params 的「数学级向下对齐」保持完全一致，
    否则 10:03 / 10:07 (15m) 会散列成两个 key，导致去重失效。
    指纹刻意 **不含 days / use_ws / use_rest / proxy_url**：
      - days 存入 meta，实现「大范围快照被小范围请求切片复用」
      - 传输方式不影响数据内容，纳入 key 只会降低命中率
    """
    tf_ms = _parse_timeframe_ms(timeframe)

    ts = pd.to_datetime(target_time_str)
    if ts.tzinfo is None:
        ts = ts.tz_localize('Asia/Shanghai')
    else:
        ts = ts.tz_convert('Asia/Shanghai')
    raw_ms = int(ts.value // 1_000_000)

    target_time_ms = raw_ms - (raw_ms % tf_ms)
    start_time_ms = target_time_ms - int(float(days) * 24 * 60 * 60 * 1000)
    target_close_time_ms = target_time_ms + tf_ms

    symbols = sorted({str(s).strip() for s in symbol_list})
    sym_hash = hashlib.md5("|".join(symbols).encode('utf-8')).hexdigest()[:12]
    tf_tag = re.sub(r'[^0-9A-Za-z]', '', str(timeframe))
    key = f"kline_{tf_tag}_{target_time_ms}_{sym_hash}"

    return {
        'key': key,
        'timeframe': str(timeframe),
        'timeframe_ms': tf_ms,
        'days': days,
        'target_time_ms': target_time_ms,
        'start_time_ms': start_time_ms,
        'target_close_time_ms': target_close_time_ms,
        'symbols': symbols,
    }


# ------------------------ 结果快照读写 ------------------------
def _snapshot_paths(snapshot_dir, key):
    return (os.path.join(snapshot_dir, f"{key}.meta.json"),
            os.path.join(snapshot_dir, f"{key}.pkl"))


def _read_kline_snapshot(sig, snapshot_dir, ttl_sec=None, incomplete_ttl_sec=600, log_prefix=""):
    """
    读取并严格校验结果快照。任何一项不满足即返回 None（视为 miss，绝不返回可疑数据）。
    校验链：meta 存在 → 版本/周期/目标时间/币种全量比对 → 覆盖范围足够 → TTL →
            pkl 可反序列化 → 逐币硬复核（必须含 target 那根 K 线 & 覆盖 start）
    """
    meta_path, data_path = _snapshot_paths(snapshot_dir, sig['key'])
    if not (os.path.exists(meta_path) and os.path.exists(data_path)):
        return None

    try:
        with open(meta_path, 'r', encoding='utf-8') as f:
            meta = json.load(f)
    except Exception:
        return None

    try:
        if int(meta.get('version', -1)) != _SNAPSHOT_VERSION:
            return None
        if str(meta.get('timeframe')) != str(sig['timeframe']):
            return None
        if int(meta.get('target_time_ms', -1)) != int(sig['target_time_ms']):
            return None
        if list(meta.get('symbols') or []) != sig['symbols']:
            return None
        snap_start_ms = int(meta.get('start_time_ms'))
        if snap_start_ms > sig['start_time_ms']:
            return None  # 快照历史深度不够，无法满足本次 days
        created_at = float(meta.get('created_at', 0))
    except Exception:
        return None

    age = max(0.0, time.time() - created_at)
    if ttl_sec is not None and age > float(ttl_sec):
        return None
    if not bool(meta.get('complete', False)) and age > float(incomplete_ttl_sec):
        logger.info(f"{log_prefix} 🧹 快照存在但数据不完整且超出保护期(age={age:.0f}s)，放弃复用，本进程将重新拉取补洞")
        return None

    try:
        with open(data_path, 'rb') as f:
            payload = pickle.load(f)
        data = payload.get('data') if isinstance(payload, dict) else None
        if not isinstance(data, dict):
            return None
    except Exception as e:
        logger.warning(f"{log_prefix} ⚠️ 快照数据文件损坏或不可读({e})，视为未命中")
        return None

    tf_ms = sig['timeframe_ms']
    need_slice = snap_start_ms < sig['start_time_ms']
    out, payload_complete = {}, True

    for sym in sig['symbols']:
        df = data.get(sym)
        if df is None or not isinstance(df, pd.DataFrame) or 'timestamp' not in df.columns:
            return None  # 币种缺失 = 结构性失配，直接 miss
        if need_slice and not df.empty:
            df = df[df['timestamp'] >= sig['start_time_ms']].reset_index(drop=True)
        # 硬复核：目标那根 K 线必须在场，且起点必须无脱节
        if df.empty or int(df['timestamp'].max()) != sig['target_time_ms'] \
                or (int(df['timestamp'].min()) - sig['start_time_ms']) > tf_ms:
            payload_complete = False
        out[sym] = df

    if not payload_complete and age > float(incomplete_ttl_sec):
        logger.info(f"{log_prefix} 🧹 快照实测不完整(age={age:.0f}s)，放弃复用")
        return None

    return out


def _write_kline_snapshot(sig, final_dfs, snapshot_dir, log_prefix=""):
    """
    Leader 交付前同步写快照（必须同步：Follower 是在 Leader 释放锁后才做双重检查的）。
    写序：先原子写 pkl（数据体），再原子写 meta.json（提交标记）。
    """
    t0 = time.time()
    os.makedirs(snapshot_dir, exist_ok=True)
    meta_path, data_path = _snapshot_paths(snapshot_dir, sig['key'])

    expected_rows = int((sig['target_time_ms'] - sig['start_time_ms']) / sig['timeframe_ms']) + 1
    data, rows, complete = {}, {}, True

    normalized = {str(k).strip(): v for k, v in (final_dfs or {}).items()}
    for sym in sig['symbols']:
        df = normalized.get(sym)
        if df is None or not isinstance(df, pd.DataFrame):
            complete = False
            continue
        data[sym] = df
        rows[sym] = int(len(df))
        if df.empty or len(df) < expected_rows or int(df['timestamp'].max()) != sig['target_time_ms']:
            complete = False

    if len(data) != len(sig['symbols']):
        logger.warning(f"{log_prefix} ⚠️ 结果币种不齐({len(data)}/{len(sig['symbols'])})，跳过快照写入以免污染缓存")
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
        logger.error(f"{log_prefix} ❌ 快照数据写入失败: {e}")
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
        logger.error(f"{log_prefix} ❌ 快照 meta 写入失败: {e}")
        return False

    logger.info(f"{log_prefix} 📸 结果快照已提交 | complete={complete} size={size_mb:.2f}MB cost={time.time() - t0:.3f}s")
    return True


# ------------------------ 垃圾回收 ------------------------
def _gc_dedupe_dirs(snapshot_dir, lock_dir, keep_sec=86400, log_prefix=""):
    now = time.time()
    removed = 0

    # 1) 过期快照（数据是幂等的，过期只是为了控制磁盘占用）
    for p in glob.glob(os.path.join(snapshot_dir, "kline_*")):
        try:
            if p.endswith('.tmp'):
                continue
            if now - os.path.getmtime(p) > keep_sec:
                os.remove(p)
                removed += 1
        except Exception:
            pass

    # 2) 残留 .tmp 碎片（1 小时以上必属僵尸）
    for pattern in (os.path.join(snapshot_dir, "*.tmp"), os.path.join(lock_dir, "*.tmp")):
        for p in glob.glob(pattern):
            try:
                if now - os.path.getmtime(p) > 3600:
                    os.remove(p)
                    removed += 1
            except Exception:
                pass

    # 3) 陈旧锁文件：仅清理 3 天以上、且能"非阻塞抢到"的锁，
    #    绝不删除可能正被持有的锁文件（POSIX 下删除被持有的锁文件会直接破坏互斥语义）
    for p in glob.glob(os.path.join(lock_dir, "kline_*.lock")):
        try:
            if now - os.path.getmtime(p) <= 3 * 86400:
                continue
            probe = InterProcessMutex(p)
            if probe.acquire(timeout=0):
                try:
                    os.remove(p)
                    removed += 1
                finally:
                    probe.release()
        except Exception:
            pass

    if removed:
        logger.info(f"{log_prefix} 🧹 去重目录 GC 完成 | removed={removed}")


def _maybe_dispatch_dedupe_gc(snapshot_dir, lock_dir, keep_sec, log_prefix=""):
    """节流 + 单进程抢占的后台 GC，绝不影响主链路耗时"""
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
# 🗄️ 模块一：缓存与存储引擎 (Cache & Storage Manager) [已修复]
# =====================================================================
def load_local_cache(symbol_list, start_time_ms, timeframe_ms, timeframe, cache_dir="data", log_prefix=""):
    """
    智能加载本地缓存数据。
    如果本地缓存涵盖了所需历史的起点，且目标区间无断层，则只需拉取缺失的增量数据；
    否则，从起点强制回拉，弥补空洞。
    【多进程加固】读盘走 _read_csv_guarded：与后台落盘线程共享同一把文件读写锁，
                  彻底规避「读到半截文件」以及 Windows 下 os.replace 被读句柄阻塞的问题。
    """
    t0 = time.time()
    memory_pool = {sym: {} for sym in symbol_list}
    fetch_since_map = {sym: start_time_ms for sym in symbol_list}
    hits, misses = 0, 0
    latest_times = {}  # 记录每个币最新的缓存时间

    for sym in symbol_list:
        safe_symbol = sym.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_{timeframe}_latest.csv")

        if os.path.exists(path):
            try:
                df = _read_csv_guarded(path, timeout=5.0, log_prefix=log_prefix)
                if df.empty or 'timestamp' not in df.columns:
                    misses += 1
                    continue

                min_ts = int(df['timestamp'].min())
                max_ts = int(df['timestamp'].max())

                # 极致性能优化：抛弃 iterrows，使用 values.tolist() 提速百倍
                records = df[['timestamp', 'open', 'high', 'low', 'close', 'volume']].values.tolist()
                for row in records:
                    ts = int(row[0])
                    memory_pool[sym][ts] = [ts] + row[1:]

                # 局部连续性校验：只检查请求的 start_time_ms 到 max_ts 这一段是否存在空洞
                if min_ts <= start_time_ms:
                    sub_df = df[df['timestamp'] >= start_time_ms]
                    if not sub_df.empty:
                        sub_min_ts = int(sub_df['timestamp'].min())
                        sub_max_ts = int(sub_df['timestamp'].max())
                        expected_rows = (sub_max_ts - sub_min_ts) // timeframe_ms + 1
                        actual_rows = len(sub_df)

                        if actual_rows >= expected_rows and (sub_min_ts - start_time_ms) <= timeframe_ms:
                            fetch_since_map[sym] = max_ts
                            hits += 1
                            latest_times[sym.split('/')[0]] = _format_bj_time(max_ts)
                        else:
                            fetch_since_map[sym] = start_time_ms
                            misses += 1
                    else:
                        fetch_since_map[sym] = start_time_ms
                        misses += 1
                else:
                    fetch_since_map[sym] = start_time_ms
                    misses += 1
            except Exception as e:
                logger.warning(f"{log_prefix} [CACHE] ⚠️ 读取 {sym} 缓存失败: {e}")
                misses += 1
        else:
            misses += 1

    cost = time.time() - t0
    # logger.info(f"{log_prefix} [CACHE] ♻️ 智能缓存装载 | hit={hits} miss={misses} load_cost={cost:.2f}s latest={latest_times}")

    return memory_pool, fetch_since_map

def _save_csv_sync_fast(full_dfs_for_cache, cache_dir, timeframe, log_prefix=""):
    """
    （后台线程专用）将内存池全量最新数据落盘。
    【多进程加固】
      1. 每个文件独占一把跨进程读写锁，与 load_local_cache 的读取严格串行；
      2. merge-on-write：写前把磁盘上可能由**其它进程**新增的行读出来合并，
         timestamp 去重时保留本次内存池的值（keep='last'），彻底消除"后写者覆盖前写者"的丢帧；
      3. 原子覆盖走 _atomic_replace，Windows 下 PermissionError 自动退避重试。
    """
    t0 = time.time()
    total_io_size = 0
    merged_extra_rows = 0
    MAX_CACHE_ROWS = 525600
    COLS = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
    os.makedirs(cache_dir, exist_ok=True)

    for symbol, df in full_dfs_for_cache.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_{timeframe}_latest.csv")

        mutex = InterProcessMutex(_csv_rw_lock_path(path))
        got_lock = mutex.acquire(timeout=60.0)  # 抢不到锁也不放弃落盘，退化为原有覆写行为
        if not got_lock:
            logger.warning(f"{log_prefix} [DISK] ⚠️ {symbol} 未取得文件锁(60s)，降级为无锁覆写")

        try:
            out_df = df
            if got_lock and os.path.exists(path):
                try:
                    # 注意：此处已持有锁，必须直连 pd.read_csv，禁止调用 _read_csv_guarded（线程锁不可重入）
                    old_df = pd.read_csv(path)
                    if not old_df.empty and 'timestamp' in old_df.columns:
                        old_df = old_df.reindex(columns=COLS)
                        new_df = df.reindex(columns=COLS)
                        before = len(new_df)
                        out_df = (pd.concat([old_df, new_df], ignore_index=True)
                                  .dropna(subset=['timestamp'])
                                  .drop_duplicates(subset=['timestamp'], keep='last')
                                  .sort_values('timestamp')
                                  .reset_index(drop=True))
                        if len(out_df) > MAX_CACHE_ROWS:
                            out_df = out_df.iloc[-MAX_CACHE_ROWS:].reset_index(drop=True)
                        merged_extra_rows += max(0, len(out_df) - before)
                except Exception as e:
                    logger.warning(f"{log_prefix} [DISK] ⚠️ {symbol} 旧缓存合并失败({e})，退化为直接覆写")
                    out_df = df

            temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
            try:
                out_df.to_csv(temp_path, index=False)
                total_io_size += os.path.getsize(temp_path)
                _atomic_replace(temp_path, path, log_prefix=log_prefix)
            except Exception as e:
                logger.error(f"{log_prefix} [DISK] ❌ 异步保存 {symbol} 失败: {e}")
                if os.path.exists(temp_path):
                    try:
                        os.remove(temp_path)
                    except Exception:
                        pass
        finally:
            if got_lock:
                mutex.release()

    cost = time.time() - t0
    logger.info(
        f"{log_prefix} [DISK] 💾 独立守护落盘完毕 | files={len(full_dfs_for_cache)} "
        f"merged_rows={merged_extra_rows} io_size={total_io_size / (1024 * 1024):.2f}MB write_cost={cost:.3f}s")

def _background_pipeline_task(memory_pool_copy, cache_dir, timeframe, log_prefix):
    """
    （被后台线程调用）承接主线程丢过来的全量脏活累活：巨量数据排序、构建 DataFrame、落盘
    """
    try:
        MAX_CACHE_ROWS = 525600
        full_dfs_for_cache = {}
        for sym, kline_dict in memory_pool_copy.items():
            all_klines = list(kline_dict.values())
            all_klines.sort(key=lambda x: x[0])

            if len(all_klines) > MAX_CACHE_ROWS:
                all_klines = all_klines[-MAX_CACHE_ROWS:]

            full_dfs_for_cache[sym] = pd.DataFrame(
                all_klines, columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )

        _save_csv_sync_fast(full_dfs_for_cache, cache_dir, timeframe, log_prefix)
    except Exception as e:
        logger.error(f"{log_prefix} [BACKGROUND_PIPE] ❌ 后台数据落盘流水线异常: {e}")


def dispatch_background_save(memory_pool_copy, timeframe, cache_dir="data", log_prefix=""):
    """
    启动独立守护线程进行全量覆盖覆写，彻底解放主线程 CPU
    """
    save_thread = threading.Thread(
        target=_background_pipeline_task,
        args=(memory_pool_copy, cache_dir, timeframe, log_prefix),
        daemon=False
    )
    save_thread.start()


# =====================================================================
# 📦 模块二：核心处理器 (Consumer) [引入防弹异常捕获与物理时钟兜底]
# =====================================================================
async def data_processor(queue, symbol_list, target_time_ms, timeframe_ms,
                         completion_event, memory_pool, processor_stats):
    reached_symbols = processor_stats["reached"]
    stats = {"HIST": 0, "WS": 0, "REST_POLL": 0}

    try:
        while True:
            symbol, kline, source, is_closed = await queue.get()
            ts = int(kline[0])
            stats[source] += 1

            memory_pool[symbol][ts] = kline

            if symbol not in reached_symbols:
                current_sys_ms = time.time() * 1000

                condition_ws_closed = (ts == target_time_ms and is_closed)
                condition_next_candle = (ts >= target_time_ms + timeframe_ms)

                condition_time_force = (ts == target_time_ms and current_sys_ms > target_time_ms + timeframe_ms + 10000)

                if condition_ws_closed or condition_next_candle or condition_time_force:
                    reached_symbols.add(symbol)
                    processor_stats["winners"][source] += 1

                    close_sys_time = datetime.now().strftime('%H:%M:%S.%f')[:-3]
                    mark = "⏱️强判" if condition_time_force and not (
                            condition_ws_closed or condition_next_candle) else ""
                    processor_stats["details"][symbol.split(':')[0]] = f"{source}{mark}({close_sys_time})"

                    if len(reached_symbols) == len(symbol_list):
                        processor_stats["throughput"] = stats
                        completion_event.set()
            queue.task_done()
    except asyncio.CancelledError:
        raise
    except Exception as e:
        logger.error(f"[Processor] ❌ 数据大脑发生致命异常: {e}，正在强制解除全局阻塞锁...")
        completion_event.set()


# =====================================================================
# 🚜 模块三：数据搬运工 (Producers) [已引入 REST 核心保护与WS动态映射]
# =====================================================================
async def fetch_historical_rest(exchange, symbol, timeframe, since_ms, queue, tracker=None):
    start_t = time.time()
    limit = 1000
    curr_since = since_ms - 60 * 60 * 1000
    total_fetched = 0
    latest_ts = 0

    while True:
        retry_count = 0
        success = False
        while retry_count <= 3:
            try:
                ohlcvs = await exchange.fetch_ohlcv(symbol, timeframe, since=curr_since, limit=limit)
                success = True
                break
            except asyncio.CancelledError:
                raise
            except Exception as e:
                retry_count += 1
                prefix = tracker.get('log_prefix', '') if tracker else ''
                logger.warning(f"{prefix} [HIST] {symbol} 拉取异常: {e}，重试 {retry_count}/3...")
                if retry_count <= 3:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"{prefix} [HIST] {symbol} 达到最大重试次数，放弃当前片段。")

        if not success or not ohlcvs:
            break

        total_fetched += len(ohlcvs)
        latest_ts = ohlcvs[-1][0]

        for k in ohlcvs: await queue.put((symbol, k, "HIST", False))

        curr_since = ohlcvs[-1][0] + 1
        if len(ohlcvs) < limit: break

    cost_t = time.time() - start_t
    if tracker is not None:
        tracker['done'] += 1
        tracker['max_cost'] = max(tracker.get('max_cost', 0), cost_t)
        tracker['fetched_rows'] += total_fetched
        tracker['latest_ts'] = max(tracker.get('latest_ts', 0), latest_ts)

        if tracker['done'] == tracker['total']:
            phase = tracker.get('phase', 'HIST')
            prefix = tracker.get('log_prefix', '')
            latest_time_str = _format_bj_time(tracker['latest_ts']) if tracker['latest_ts'] > 0 else 'N/A'
            # logger.info(
            #     f"{prefix} [{phase}] 📦 缺口历史补齐就绪 | done={tracker['done']}/{tracker['total']} fetched_rows={tracker['fetched_rows']} max_cost={tracker['max_cost']:.2f}s latest_time={latest_time_str}")


async def fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=""):
    ws_mapping = {s.replace("/", "").split(":")[0].upper(): s for s in symbol_list}
    ws_symbols = [k.lower() for k in ws_mapping.keys()]
    stream_url = f"wss://fstream.binance.com/market/stream?streams={'/'.join([f'{s}@kline_{timeframe}' for s in ws_symbols])}"

    attempt = 0
    while True:
        try:
            async with aiohttp.ClientSession() as session:
                async with session.ws_connect(stream_url, proxy=proxy_url, heartbeat=10) as ws:
                    # logger.info(f"{log_prefix} [WSS] ✅ 数据总线已建连 | streams={len(ws_symbols)}")
                    attempt = 0  # 成功连接并准备接收数据，重置退避计数
                    async for msg in ws:
                        if msg.type == aiohttp.WSMsgType.TEXT:
                            data = json.loads(msg.data)
                            if 'data' in data and 'k' in data['data']:
                                k_data = data['data']['k']
                                raw_s = data['data']['s']

                                if raw_s in ws_mapping:
                                    target_symbol = ws_mapping[raw_s]
                                    kline = [int(k_data['t']), float(k_data['o']), float(k_data['h']),
                                             float(k_data['l']), float(k_data['c']), float(k_data['v'])]
                                    await queue.put((target_symbol, kline, "WS", bool(k_data['x'])))
        except asyncio.CancelledError:
            raise
        except Exception as e:
            attempt += 1
            sleep_time = min(2 ** attempt, 16)  # 指数退避重试：2, 4, 8, 16...
            logger.error(f"{log_prefix} [WSS] ❌ 异常断开: {e} | {sleep_time}秒后自愈重连 (第{attempt}次)...")
            await asyncio.sleep(sleep_time)


async def fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue):
    try:
        while True:
            tasks = [exchange.fetch_ohlcv(sym, timeframe, limit=2) for sym in symbol_list]
            results = await asyncio.gather(*tasks, return_exceptions=True)
            for sym, ohlcvs in zip(symbol_list, results):
                if isinstance(ohlcvs, Exception) or not ohlcvs: continue

                latest_ts = ohlcvs[-1][0]
                for k in ohlcvs:
                    is_closed = (k[0] < latest_ts)
                    await queue.put((sym, k, "REST_POLL", is_closed))

            # [改造点3] 已彻底剥离 await asyncio.sleep，配合最后5秒启动机制，实现无延迟的极速脉冲轮询
    except asyncio.CancelledError:
        raise


# =====================================================================
# 🧠 模块四：中央大脑与流程编排 (Task Orchestrator) [引入极致时间调度]
# =====================================================================
def parse_time_params(exchange, timeframe, days, target_time_str):
    # 1. 计算 K 线周期的绝对毫秒数
    timeframe_ms = exchange.parse_timeframe(timeframe) * 1000

    # 2. 将传入的字符串时间转化为原始时间戳
    raw_target_time_ms = int(pd.to_datetime(target_time_str).tz_localize('Asia/Shanghai').timestamp() * 1000)

    # 3. 【核心修正】数学级向下对齐，算出多余的“零头”时间
    remainder = raw_target_time_ms % timeframe_ms

    if remainder != 0:
        # 如果有余数，说明时间没对齐，强行减去余数抹平
        target_time_ms = raw_target_time_ms - remainder

        # 将毫秒转换回北京时间字符串，用于友好打印告警
        raw_str = pd.to_datetime(raw_target_time_ms, unit='ms', utc=True).tz_convert('Asia/Shanghai').strftime(
            '%Y-%m-%d %H:%M:%S')
        aligned_str = pd.to_datetime(target_time_ms, unit='ms', utc=True).tz_convert('Asia/Shanghai').strftime(
            '%Y-%m-%d %H:%M:%S')

        logger.info("\n" + "=" * 60)
        logger.info(f"🛡️ [时间防弹装甲] 触发时间边界对齐干预！")
        logger.info(f"   ⚠️ 原始请求时间 : {raw_str} (不符合 {timeframe} 切片标准)")
        logger.info(f"   ✂️ 抹除多余零头 : 减去 {remainder} 毫秒")
        logger.info(f"   ✅ 强制向下对齐 : {aligned_str}")
        logger.info("=" * 60 + "\n")
    else:
        # 本身已经完美对齐，不需要修正
        target_time_ms = raw_target_time_ms

    # 4. 基于对齐后的时间，计算起点和收盘点
    start_time_ms = target_time_ms - (days * 24 * 60 * 60 * 1000)
    target_close_time_ms = target_time_ms + timeframe_ms

    return timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms


async def check_time_sync(exchange, log_prefix=""):
    """
    检测本地物理机时间与 Binance 服务器时间的精准偏差与网络往返延迟 (RTT)。
    采用标准 NTP 估计算法剔除网络传输误差。引入重试防止闪断。
    """
    try:
        t0, server_time, t1 = 0, 0, 0
        for attempt in range(3):
            try:
                t0 = time.time() * 1000
                server_time = await exchange.fetch_time()
                t1 = time.time() * 1000
                break
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(1)
                else:
                    raise e

        rtt = t1 - t0
        local_time_at_server = t0 + (rtt / 2)
        offset = server_time - local_time_at_server
        status = "落后" if offset > 0 else "超前"

        logger.info(
            f"{log_prefix} [PING] ⏱️ 时钟与网络基准测试 | RTT延迟: {rtt:.2f}ms | 本地时钟{status}服务器: {abs(offset):.2f}ms")

        if abs(offset) > 500:
            logger.warning(
                f"{log_prefix} [PING] ⚠️ 极高危预警：本地时间偏差过大(>{abs(offset):.0f}ms)！极易导致 API 签名失败，请立即执行 NTP 时间同步！")

        return offset, rtt

    except Exception as e:
        logger.error(f"{log_prefix} [PING] ❌ 时钟同步检测失败: {e}")
        return None, None


async def _async_core_sniping_orchestrator(symbol_list, timeframe, days, target_time_str,
                                           use_ws, use_rest, proxy_url):
    orchestrator_start_t = time.time()
    run_id = f"T-{uuid.uuid4().hex[:4].upper()}"
    log_prefix = f"[{run_id}]"

    # 1. 定义基础配置
    exchange_config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 15000  # 核心防断网：提高超时时间，允许网络小幅波段
    }

    # 2. 如果 proxy_url 存在（非 None 且非空），则动态注入代理配置
    if proxy_url:
        exchange_config['aiohttp_proxy'] = proxy_url
        exchange_config['proxies'] = {
            'http': proxy_url,
            'https': proxy_url
        }

    # 3. 使用配置字典初始化 Exchange
    exchange = ccxt.binance(exchange_config)
    try:
        # 核心防断网装甲：load_markets 增加重试保护
        for attempt in range(3):
            try:
                await exchange.load_markets()
                break
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, Exception) as e:
                if attempt < 2:
                    wait_time = 2 ** attempt
                    logger.warning(f"{log_prefix} [INIT] load_markets 异常: {e} | {wait_time}s后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"{log_prefix} [INIT] load_markets 彻底失败，将抛出给上层接管: {e}")
                    raise

        # await check_time_sync(exchange, log_prefix)
        # 1. 计算时间参数
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)

        # logger.info(f"{log_prefix} [INIT] 🚀 极速引擎发车 | target={_format_bj_time(target_time_ms)}(+0800) symbols={symbol_list} days={days}")

        # 2. 智能缓存装载 & 内存池初始化
        memory_pool, fetch_since_map = load_local_cache(symbol_list, start_time_ms, timeframe_ms, timeframe,
                                                        log_prefix=log_prefix)

        queue = asyncio.Queue()
        completion_event = asyncio.Event()
        processor_stats = {"reached": set(), "winners": {"WS": 0, "REST_POLL": 0, "HIST": 0}, "throughput": {},
                           "details": {}}

        # 3. 启动后台协程任务 (首波历史追赶)
        processor_task = asyncio.create_task(
            data_processor(queue, symbol_list, target_time_ms, timeframe_ms, completion_event, memory_pool,
                           processor_stats)
        )

        hist_tracker_1 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0, 'phase': 'HIST-1',
                          'log_prefix': log_prefix, 'latest_ts': 0}
        history_tasks = [
            asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, fetch_since_map[sym], queue, hist_tracker_1))
            for sym in symbol_list
        ]

        # 4. 战术休眠第一阶段：提前一分钟休眠至目标时间到来，随后立刻触发二次追赶释放压力
        sleep_to_target = target_time_ms - exchange.milliseconds()
        if sleep_to_target > 0:
            logger.info(
                f"{log_prefix} [SYNC] 💤 进入一阶段战术休眠 | sleep={sleep_to_target / 1000:.1f}s next_action={_format_bj_time(target_time_ms)}")
            await asyncio.sleep(sleep_to_target / 1000)

        hist_tracker_2 = {'done': 0, 'total': len(symbol_list), 'max_cost': 0, 'fetched_rows': 0, 'phase': 'HIST-2',
                          'log_prefix': log_prefix, 'latest_ts': 0}
        gap_tasks = []
        for sym in symbol_list:
            gap_start_ms = max(memory_pool[sym].keys()) if memory_pool[sym] else fetch_since_map[sym]
            gap_tasks.append(asyncio.create_task(
                fetch_historical_rest(exchange, sym, timeframe, gap_start_ms, queue, hist_tracker_2)
            ))

        history_tasks.extend(gap_tasks)

        # 实时双擎机制分配：WS流可以即刻点火，监听收线全过程
        engine_tasks = []
        if use_ws:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_ws(symbol_list, timeframe, queue, proxy_url, log_prefix=log_prefix)))

        # 战术休眠第二阶段：死等最后 5 秒，再瞬间点爆无延迟脉冲 REST
        sleep_to_rest = target_close_time_ms - 5000 - exchange.milliseconds()
        if sleep_to_rest > 0:
            # logger.info(f"{log_prefix} [SYNC] 💤 挂起等待收线冲刺(最后5s) | sleep={sleep_to_rest / 1000:.1f}s next_action=脉冲轮询兜底")
            await asyncio.sleep(sleep_to_rest / 1000)

        if use_rest:
            engine_tasks.append(
                asyncio.create_task(fetch_realtime_rest_polling(exchange, symbol_list, timeframe, queue)))

        # 5. 超时检测预警与收盘等待
        try:
            absolute_deadline_ms = target_close_time_ms + 60000
            current_ms = exchange.milliseconds()
            timeout = max(0.1, (absolute_deadline_ms - current_ms) / 1000)
            await asyncio.wait_for(completion_event.wait(), timeout=timeout)
        except asyncio.TimeoutError:
            lag_symbols = set(symbol_list) - processor_stats["reached"]
            logger.warning(
                f"{log_prefix} [RACE] 🚨 触发绝对超时硬熔断(>1m) | pending={list(lag_symbols)[:3]}... 强制脱壳返回！")

        # 【重点修改】：在这里确保所有尽力而为的历史拉取已交卷，保证最后交付的数据尽可能补全缺口
        await asyncio.gather(*history_tasks, return_exceptions=True)
        await queue.join()
        # [核心竞速指标计算]
        close_latency_ms = exchange.milliseconds() - target_close_time_ms
        tp = processor_stats.get('throughput', {})
        win = processor_stats['winners']
        det = processor_stats['details']
        logger.info(
            f"{log_prefix} [RACE] 🎯 目标全线闭合 | close_latency={close_latency_ms / 1000:.3f}s winner=(WS:{win['WS']}, REST:{win['REST_POLL']}) throughput=(ws:{tp.get('WS', 0)}, rest:{tp.get('REST_POLL', 0)}, hist:{tp.get('HIST', 0)}) details={det}")

        # 6. 发令枪响：瞬间强杀所有底层协程 (只杀双擎和处理器，不杀已经跑完的历史任务)
        all_tasks = engine_tasks + [processor_task]
        for task in all_tasks:
            if not task.done(): task.cancel()

        await asyncio.gather(*all_tasks, return_exceptions=True)

        # =====================================================================
        # 🚀 7. CPU 极限优化段：主线程金蝉脱壳、零阻塞构建极简返回数据
        # =====================================================================
        final_dfs = {}
        expected_rows = int((target_time_ms - start_time_ms) / timeframe_ms) + 1

        for sym in symbol_list:
            # 第一层防线：极速字典推导式过滤（取代动辄几十万行的全量循环与判断）
            sliced_klines = [
                k for ts, k in memory_pool[sym].items()
                if start_time_ms <= ts <= target_time_ms
            ]
            # 第二层防线：局部极速排序（取代全量排序）
            sliced_klines.sort(key=lambda x: x[0])

            # 第三层防线：极简构建 Pandas (直接摒弃时区转换)
            sliced_df = pd.DataFrame(
                sliced_klines,
                columns=['timestamp', 'open', 'high', 'low', 'close', 'volume']
            )
            final_dfs[sym] = sliced_df

            # 数据完整性比对告警
            actual_rows = len(sliced_klines)
            if actual_rows < expected_rows:
                logger.warning(
                    f"{log_prefix} [CHECK] ⚠️ {sym} 数据存在断缺！预期 {expected_rows} 条，实际 {actual_rows} 条 (缺失 {expected_rows - actual_rows} 条)")

        # =====================================================================
        # 🤝 8. 后台交接：把沉重的 50+ 万条全量清洗与落盘，扔给子线程慢慢跑
        # =====================================================================
        memory_pool_copy = {sym: pool.copy() for sym, pool in memory_pool.items()}
        dispatch_background_save(memory_pool_copy, timeframe, cache_dir="data", log_prefix=log_prefix)

        total_pts = sum(len(df) for df in final_dfs.values())
        total_runtime = time.time() - orchestrator_start_t
        logger.info(
            f"{log_prefix} [EXIT] 🎉 主任务零阻塞闪现交付 | range=[{_format_bj_time(start_time_ms)} ~ {_format_bj_time(target_time_ms)}] total_rows={total_pts} runtime={total_runtime:.2f}s")
        return final_dfs

    finally:
        try:
            # 1. 物理斩断：加上 await，因为新版 aiohttp 中它是协程！
            if hasattr(exchange, 'session') and exchange.session:
                if hasattr(exchange.session, 'connector') and exchange.session.connector:
                    try:
                        # 只给 2 毫秒的死线，强行触发关闭动作
                        await asyncio.wait_for(exchange.session.connector.close(), timeout=0.00002)
                    except Exception:
                        pass  # 超时直接静默，此时物理连接已被撕裂

            # 2. 欺骗 CCXT 析构函数，防止它检查 Session 触发长篇警告
            exchange.session = None

            # 3. 象征性走一下 CCXT 的 close，2 毫秒必杀
            await asyncio.wait_for(exchange.close(), timeout=0.00002)

        except Exception:
            pass  # 屏蔽一切退出时的报错，实现完美脱壳


# =====================================================================
# 🗄️ 模块五：资金费率专属缓存与存储 (Funding Rate Cache & Storage)
# =====================================================================
def load_funding_cache(symbol_list, cache_dir="data", log_prefix=""):
    """
    针对资金费率的智能缓存加载，基于字典无脑覆盖，天然去重
    """
    memory_pool = {sym: {} for sym in symbol_list}
    max_cache_ts_map = {sym: 0 for sym in symbol_list}
    os.makedirs(cache_dir, exist_ok=True)

    for sym in symbol_list:
        safe_symbol = sym.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_funding_latest.csv")

        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if not df.empty and 'timestamp' in df.columns:
                    # 极速遍历塞入哈希表
                    records = df[['timestamp', 'fundingRate', 'symbol']].values.tolist()
                    for row in records:
                        ts = int(row[0])
                        memory_pool[sym][ts] = {
                            'timestamp': ts,
                            'fundingRate': float(row[1]),
                            'symbol': row[2]
                        }
                    max_cache_ts_map[sym] = int(df['timestamp'].max())
            except Exception as e:
                logger.warning(f"{log_prefix} [FUNDING_CACHE] ⚠️ 读取 {sym} 资金费率缓存失败: {e}")

    return memory_pool, max_cache_ts_map


def _save_funding_csv_sync_fast(full_dfs, cache_dir, log_prefix=""):
    """后台独立落盘资金费率，采用临时文件防碎裂技术"""
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in full_dfs.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_funding_latest.csv")
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            df.to_csv(temp_path, index=False)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"{log_prefix} [FUNDING_DISK] ❌ 保存 {symbol} 资金费率失败: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass


def dispatch_funding_background_save(memory_pool_copy, cache_dir="data", log_prefix=""):
    """脱壳资金费率落盘任务"""

    def _task():
        full_dfs = {}
        for sym, records in memory_pool_copy.items():
            # 按 timestamp 排序
            sorted_records = [r for _, r in sorted(records.items())]
            full_dfs[sym] = pd.DataFrame(sorted_records, columns=['timestamp', 'fundingRate', 'symbol'])
        _save_funding_csv_sync_fast(full_dfs, cache_dir, log_prefix)

    threading.Thread(target=_task, daemon=False).start()


# =====================================================================
# 🎯 模块六：资金费率极速获取引擎 (Funding Rate Orchestrator)
# =====================================================================
async def _fetch_funding_for_symbol(exchange, symbol, target_start_ms, memory_pool, max_cache_ts, threshold_ms=60000,
                                    log_prefix=""):
    """
    单币种资金费率获取策略：
    - 阈值内（如1m内结算）：战术休眠至T-5s，脉冲轮询最新数据
    - 阈值外：自 最大缓存时间往前推24h 拉取进行重叠拼接
    """
    try:
        # 核心防断网：给获取基础信息的请求覆盖一层重试，防止一抖动整个币种跳过
        funding_info = None
        for attempt in range(3):
            try:
                funding_info = await exchange.fetch_funding_rate(symbol)
                break
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2)
                else:
                    logger.error(f"{log_prefix} [FUNDING_ERR] 获取 {symbol} 资金费率基础信息彻底失败: {e}")
                    return

        if not funding_info:
            return

        # 【深度兼容补丁】兼容 CCXT 针对 Binance 各版本的字段映射差异，强行深挖原始字段
        next_funding_time = funding_info.get('nextFundingTimestamp')
        if not next_funding_time:
            next_funding_time = funding_info.get('fundingTimestamp')
        if not next_funding_time and 'info' in funding_info:
            raw_info = funding_info['info']
            if 'nextFundingTime' in raw_info:
                next_funding_time = int(raw_info['nextFundingTime'])

        server_time = exchange.milliseconds()

        # 容错：如果深度穿透后交易所依然没有返回 nextFundingTimestamp，强行触发场景 B
        time_to_funding = (next_funding_time - server_time) if next_funding_time else -1

        if 0 < time_to_funding <= threshold_ms:
            # ⚡ 场景 A：临近结算时间 (进入狙击模式)
            sleep_sec = max(0, (time_to_funding - 5000) / 1000.0)
            logger.info(
                f"{log_prefix} [FUNDING_SNIPE] 🎯 {symbol} 距离结算仅 {time_to_funding / 1000:.1f}s，战术休眠 {sleep_sec:.1f}s 后启动脉冲探测...")
            await asyncio.sleep(sleep_sec)

            retry_count = 0
            while retry_count < 20:  # 保护性熔断退出，防止死循环
                try:
                    # 安全起见，since 取前一日，以防交易所默认只给一条旧的
                    since_ms = next_funding_time - (24 * 60 * 60 * 1000)
                    hist_rates = await exchange.fetch_funding_rate_history(symbol, since=since_ms, limit=100)

                    if hist_rates:
                        max_fetched_ts = max(r['timestamp'] for r in hist_rates)
                        # 先塞入缓存去重
                        for r in hist_rates:
                            ts = r['timestamp']
                            memory_pool[symbol][ts] = {'timestamp': ts, 'fundingRate': r['fundingRate'],
                                                       'symbol': symbol}

                        # 判断是否已经捕捉到了最新的那个交割点
                        if max_fetched_ts >= next_funding_time:
                            logger.info(
                                f"{log_prefix} [FUNDING_SNIPE] 🔫 {symbol} 脉冲命中！获取到最新结算费率，时间: {_format_bj_time(max_fetched_ts)}")
                            break
                except Exception as e:
                    # 核心防断网：局部捕获脉冲异常，避免直接抛到外层终止探测
                    logger.warning(f"{log_prefix} [FUNDING_SNIPE] {symbol} 脉冲抓取异常: {e}，将在下次循环重试...")

                retry_count += 1
                await asyncio.sleep(0.5)  # 极速脉冲
        else:
            # 🛡️ 场景 B：平稳期 (安全重叠回补)
            since_ms = max_cache_ts - (24 * 60 * 60 * 1000) if max_cache_ts > 0 else target_start_ms

            # 格式化时间与阈值信息（包含缺失时间戳的容错）
            next_time_str = _format_bj_time(next_funding_time) if next_funding_time else "未知"
            dist_str = f"{time_to_funding / 1000:.1f}s" if time_to_funding > 0 else "未知"
            threshold_sec = threshold_ms / 1000

            logger.info(
                f"{log_prefix} [FUNDING_REST] 🛒 {symbol} 距离结算较远 | "
                f"下次结算: {next_time_str} (距今 {dist_str}) > 阈值({threshold_sec}s) | "
                f"执行常规回补 since: {_format_bj_time(since_ms)}"
            )

            # 循环分页拉取直到最新
            curr_since = since_ms
            while True:
                hist_rates = None
                # 核心防断网：为每一次分页请求提供重试保护
                for attempt in range(3):
                    try:
                        hist_rates = await exchange.fetch_funding_rate_history(symbol, since=curr_since, limit=1000)
                        break
                    except Exception as e:
                        if attempt < 2:
                            await asyncio.sleep(2)
                        else:
                            logger.error(f"{log_prefix} [FUNDING_REST] {symbol} 历史分页请求中断: {e}")
                            raise e  # 彻底失败，交由外层 except 处理

                if not hist_rates:
                    break

                for r in hist_rates:
                    ts = r['timestamp']
                    memory_pool[symbol][ts] = {'timestamp': ts, 'fundingRate': r['fundingRate'], 'symbol': symbol}

                if len(hist_rates) < 1000:
                    break
                curr_since = hist_rates[-1]['timestamp'] + 1

    except Exception as e:
        logger.error(f"{log_prefix} [FUNDING_ERR] ❌ {symbol} 资金费率获取异常退出: {e}")


async def _async_core_funding_orchestrator(symbol_list, days, proxy_url):
    orchestrator_start_t = time.time()
    run_id = f"F-{uuid.uuid4().hex[:4].upper()}"
    log_prefix = f"[{run_id}]"

    exchange_config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 15000  # 核心防断网
    }
    if proxy_url:
        exchange_config['aiohttp_proxy'] = proxy_url
        exchange_config['proxies'] = {'http': proxy_url, 'https': proxy_url}

    exchange = ccxt.binance(exchange_config)
    try:
        # 核心防断网装甲：资金费率引擎同步覆盖 load_markets 保护
        for attempt in range(3):
            try:
                await exchange.load_markets()
                break
            except (ccxt.NetworkError, ccxt.ExchangeNotAvailable, ccxt.RequestTimeout, Exception) as e:
                if attempt < 2:
                    wait_time = 2 ** attempt
                    logger.warning(f"{log_prefix} [FUNDING_INIT] load_markets 异常: {e} | {wait_time}s后重试...")
                    await asyncio.sleep(wait_time)
                else:
                    logger.error(f"{log_prefix} [FUNDING_INIT] load_markets 彻底失败: {e}")
                    raise

        # 核心修改：不再使用传入的 target_time，直接获取当前服务器时间作为最新右边界
        target_end_ms = exchange.milliseconds()
        target_start_ms = target_end_ms - (days * 24 * 60 * 60 * 1000)
        target_end_str = _format_bj_time(target_end_ms)

        logger.info(
            f"{log_prefix} [FUNDING_INIT] 🚀 资金费率极速引擎发车 | target_end={target_end_str} days={days} symbols={len(symbol_list)}")

        # 1. 智能加载缓存
        memory_pool, max_cache_ts_map = load_funding_cache(symbol_list, log_prefix=log_prefix)

        # 2. 并发执行每个币种的资金费率拉取（内置阈值分流判断）
        # 资金费率的1分钟阈值 = 60000 毫秒
        tasks = [
            _fetch_funding_for_symbol(exchange, sym, target_start_ms, memory_pool, max_cache_ts_map[sym], 60000,
                                      log_prefix)
            for sym in symbol_list
        ]
        await asyncio.gather(*tasks)

        # 3. 后台守护线程异步落盘
        memory_pool_copy = {sym: {k: v.copy() for k, v in pool.items()} for sym, pool in memory_pool.items()}
        dispatch_funding_background_save(memory_pool_copy, log_prefix=log_prefix)

        # 4. 闪现交付 O(1) 切片
        final_dfs = {}
        total_pts = 0
        for sym in symbol_list:
            sliced_records = [
                r for ts, r in memory_pool[sym].items()
                if target_start_ms <= ts <= target_end_ms
            ]
            # 局部排序
            sliced_records.sort(key=lambda x: x['timestamp'])
            df = pd.DataFrame(sliced_records, columns=['timestamp', 'fundingRate', 'symbol'])
            final_dfs[sym] = df
            total_pts += len(df)

        total_runtime = time.time() - orchestrator_start_t
        logger.info(
            f"{log_prefix} [FUNDING_EXIT] 🎉 资金费率极速交付完毕 | total_rows={total_pts} runtime={total_runtime:.2f}s")
        return final_dfs
    finally:
        # 物理斩断释放内存
        try:
            if hasattr(exchange, 'session') and exchange.session:
                if hasattr(exchange.session, 'connector') and exchange.session.connector:
                    try:
                        await asyncio.wait_for(exchange.session.connector.close(), timeout=0.00002)
                    except:
                        pass
            exchange.session = None
            await asyncio.wait_for(exchange.close(), timeout=0.00002)
        except:
            pass


# =====================================================================
# 📊 模块七：OI 未平仓合约缓存与存储引擎 (Open Interest Cache & Storage)
# =====================================================================
def load_oi_cache(symbol_list, timeframe, cache_dir="data", log_prefix=""):
    """
    智能加载 OI 本地缓存，严禁多余字段，仅提取并保留 timestamp 和 oi_amount。
    """
    memory_pool = {sym: {} for sym in symbol_list}
    max_cache_ts_map = {sym: 0 for sym in symbol_list}
    os.makedirs(cache_dir, exist_ok=True)

    for sym in symbol_list:
        safe_symbol = sym.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_oi_{timeframe}_latest.csv")

        if os.path.exists(path):
            try:
                df = pd.read_csv(path)
                if not df.empty and 'timestamp' in df.columns and 'oi_amount' in df.columns:
                    records = df[['timestamp', 'oi_amount']].values.tolist()
                    for row in records:
                        ts = int(row[0])
                        memory_pool[sym][ts] = {
                            'timestamp': ts,
                            'oi_amount': float(row[1]) if pd.notna(row[1]) else 0.0
                        }
                    max_cache_ts_map[sym] = int(df['timestamp'].max())
            except Exception as e:
                logger.warning(f"{log_prefix} [OI_CACHE] ⚠️ 读取 {sym} OI 缓存失败: {e}")

    return memory_pool, max_cache_ts_map


def _save_oi_csv_sync_fast(full_dfs, cache_dir, timeframe, log_prefix=""):
    """后台独立落盘 OI 数据，严格保障双列极简格式，辅以临时文件防损坏"""
    os.makedirs(cache_dir, exist_ok=True)
    for symbol, df in full_dfs.items():
        safe_symbol = symbol.replace("/", "_").replace(":", "_")
        path = os.path.join(cache_dir, f"{safe_symbol}_oi_{timeframe}_latest.csv")
        temp_path = f"{path}.{uuid.uuid4().hex}.tmp"
        try:
            # df 中本身已被修剪为纯净两列
            df.to_csv(temp_path, index=False)
            os.replace(temp_path, path)
        except Exception as e:
            logger.error(f"{log_prefix} [OI_DISK] ❌ 保存 {symbol} OI 失败: {e}")
            if os.path.exists(temp_path):
                try:
                    os.remove(temp_path)
                except:
                    pass


def dispatch_oi_background_save(memory_pool_copy, timeframe, cache_dir="data", log_prefix=""):
    """释放主线程的 OI 落盘任务"""

    def _task():
        full_dfs = {}
        for sym, records in memory_pool_copy.items():
            sorted_records = [r for _, r in sorted(records.items())]
            full_dfs[sym] = pd.DataFrame(
                sorted_records,
                columns=['timestamp', 'oi_amount']
            )
        _save_oi_csv_sync_fast(full_dfs, cache_dir, timeframe, log_prefix)

    threading.Thread(target=_task, daemon=False).start()


# =====================================================================
# 🚀 模块八：OI 未平仓合约极速获取引擎 (Open Interest Orchestrator)
# =====================================================================
async def _fetch_oi_for_symbol(exchange, symbol, timeframe, target_start_ms, target_time_ms, memory_pool, max_cache_ts,
                               log_prefix=""):
    """
    单币种 OI 获取：纯 REST API 分页轮询拉取，受到目标时间硬性边界约束。
    智能实现历史追赶、战术休眠以及目标时间到达后的脉冲探测，保证必须拉取到 target_time_ms 的数据。
    """
    try:
        # 如果缓存有数据，从缓存最大时间回退两个 timeframe 作为安全拼接点；否则从起跑线拉取
        timeframe_ms = exchange.parse_timeframe(timeframe) * 1000
        since_ms = max_cache_ts - (timeframe_ms * 2) if max_cache_ts > 0 else target_start_ms
        curr_since = since_ms

        logger.info(
            f"{log_prefix} [OI_REST] 🛒 {symbol} 启动纯 REST 轮询拉取 OI | 起点: {_format_bj_time(curr_since)} | 目标: {_format_bj_time(target_time_ms)}")

        while True:
            hist_oi = None
            # 核心防断网装甲：三次指数退避重试
            for attempt in range(3):
                try:
                    # limit=500 是 Binance 等交易所对 OI 接口的标准常见限制
                    hist_oi = await exchange.fetch_open_interest_history(symbol, timeframe, since=curr_since, limit=500)
                    break
                except asyncio.CancelledError:
                    raise
                except Exception as e:
                    if attempt < 2:
                        await asyncio.sleep(2)
                    else:
                        logger.error(f"{log_prefix} [OI_REST] {symbol} OI 历史分页请求中断: {e}")
                        raise e

            if hist_oi:
                for r in hist_oi:
                    ts = int(r['timestamp'])
                    # 包容性提取：CCXT 返回的 openInterestAmount 即为币的持仓量
                    amt = r.get('openInterestAmount', 0.0)

                    # 如果标准字段提取失败，尝试兼容 info 中的原始数据
                    if (amt is None or amt == 0.0) and 'info' in r:
                        raw_sum_amt = r['info'].get('sumOpenInterest')
                        if raw_sum_amt: amt = float(raw_sum_amt)

                    # 严格按照两列格式写入内存池
                    memory_pool[symbol][ts] = {
                        'timestamp': ts,
                        'oi_amount': amt if amt else 0.0
                    }

                latest_ts = hist_oi[-1]['timestamp']

                # 【核心修正】：如果最新数据已经覆盖或超越了目标时间，大功告成，直接跳出
                if latest_ts >= target_time_ms:
                    logger.info(
                        f"{log_prefix} [OI_REST] 🎯 {symbol} 成功捕获目标时间 {_format_bj_time(target_time_ms)} 的 OI 数据！")
                    break

                # 如果拉满了 500 条且还未到目标时间，说明历史还没拉完，直接继续拉下一页
                if len(hist_oi) == 500:
                    curr_since = latest_ts + 1
                    continue
                else:
                    curr_since = latest_ts + 1
            else:
                # 没有拉到数据，兜底检查内存池最高水位是否已经满足目标（防死循环）
                if memory_pool[symbol] and max(memory_pool[symbol].keys()) >= target_time_ms:
                    break

            # =====================================================================
            # 执行到这里，说明历史数据已经拉到【当前最新】，但【当前最新】还没有达到目标时间 target_time_ms。
            # 需要进入【挂起/轮询】阶段，死等目标数据产出。
            # =====================================================================
            current_sys_ms = time.time() * 1000
            time_to_wait = target_time_ms - current_sys_ms

            if time_to_wait > 0:
                # 场景 A：目标时间还在未来，进入战术休眠直到目标时间到达（加 1 秒防提前苏醒）
                sleep_sec = (time_to_wait / 1000.0) + 1.0
                logger.info(f"{log_prefix} [OI_REST] 💤 {symbol} 目标未到，休眠 {sleep_sec:.1f}s 后启动冲刺探测...")
                await asyncio.sleep(sleep_sec)
            else:
                # 场景 B：目标时间已过，但交易所还没刷新出该时间点的数据（延迟），启动高频脉冲轮询
                await asyncio.sleep(1.0)  # 2秒轮询一次，兼顾极限速度与防封

    except asyncio.CancelledError:
        # 响应外部的超时熔断
        raise
    except Exception as e:
        logger.error(f"{log_prefix} [OI_ERR] ❌ {symbol} OI 获取异常退出: {e}")


async def _async_core_oi_orchestrator(symbol_list, timeframe, days, target_time_str, proxy_url):
    orchestrator_start_t = time.time()
    run_id = f"O-{uuid.uuid4().hex[:4].upper()}"
    log_prefix = f"[{run_id}]"

    exchange_config = {
        'enableRateLimit': True,
        'options': {'defaultType': 'swap'},
        'timeout': 15000
    }
    if proxy_url:
        exchange_config['aiohttp_proxy'] = proxy_url
        exchange_config['proxies'] = {'http': proxy_url, 'https': proxy_url}

    exchange = ccxt.binance(exchange_config)
    try:
        # 防断网加载 Markets
        for attempt in range(3):
            try:
                await exchange.load_markets()
                break
            except Exception as e:
                if attempt < 2:
                    await asyncio.sleep(2 ** attempt)
                else:
                    logger.error(f"{log_prefix} [OI_INIT] load_markets 彻底失败: {e}")
                    raise

        # 调用已有的 parse_time_params，实现数学级向下对齐，算出严苛时间边界
        timeframe_ms, target_time_ms, start_time_ms, target_close_time_ms = parse_time_params(
            exchange, timeframe, days, target_time_str)

        logger.info(
            f"{log_prefix} [OI_INIT] 🚀 OI 未平仓极速引擎发车 | target={_format_bj_time(target_time_ms)} timeframe={timeframe} days={days} symbols={len(symbol_list)}")

        # 1. 智能加载双列缓存
        memory_pool, max_cache_ts_map = load_oi_cache(symbol_list, timeframe, log_prefix=log_prefix)

        # 2. 并发执行所有币种的 OI 纯 REST 轮询拉取（封装成 Task 以便支持超时熔断）
        tasks = [
            asyncio.create_task(
                _fetch_oi_for_symbol(exchange, sym, timeframe, start_time_ms, target_close_time_ms, memory_pool,
                                     max_cache_ts_map[sym], log_prefix)
            )
            for sym in symbol_list
        ]

        # 绝对超时限制：与K线引擎对齐，目标收盘时间 + 60秒宽限期，防止API长时假死
        absolute_deadline_ms = target_close_time_ms + 60000
        current_ms = exchange.milliseconds()
        timeout = max(5.0, (absolute_deadline_ms - current_ms) / 1000.0)

        try:
            # 等待所有任务完成，或者触发绝对超时熔断
            await asyncio.wait_for(asyncio.gather(*tasks), timeout=timeout)
        except asyncio.TimeoutError:
            logger.warning(f"{log_prefix} [OI_RACE] 🚨 触发绝对超时硬熔断(>1m)！强制终止脉冲轮询，交卷返回当前已获取数据！")
            for task in tasks:
                if not task.done():
                    task.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)

        # 3. 后台守护线程异步落盘 (仅保存 timestamp, oi_amount)
        memory_pool_copy = {sym: {k: v.copy() for k, v in pool.items()} for sym, pool in memory_pool.items()}
        dispatch_oi_background_save(memory_pool_copy, timeframe, log_prefix=log_prefix)

        # 4. 闪现交付 O(1) 切片 (仅返回 timestamp, oi_amount 两列)
        final_dfs = {}
        total_pts = 0
        for sym in symbol_list:
            sliced_records = [
                r for ts, r in memory_pool[sym].items()
                if start_time_ms <= ts <= target_time_ms
            ]
            sliced_records.sort(key=lambda x: x['timestamp'])

            # DataFrame 彻底净化为两列
            df = pd.DataFrame(sliced_records, columns=['timestamp', 'oi_amount'])
            final_dfs[sym] = df
            total_pts += len(df)

        total_runtime = time.time() - orchestrator_start_t
        logger.info(
            f"{log_prefix} [OI_EXIT] 🎉 OI 纯REST极速交付完毕 | total_rows={total_pts} runtime={total_runtime:.2f}s")
        return final_dfs
    finally:
        try:
            if hasattr(exchange, 'session') and exchange.session:
                if hasattr(exchange.session, 'connector') and exchange.session.connector:
                    try:
                        await asyncio.wait_for(exchange.session.connector.close(), timeout=0.00002)
                    except:
                        pass
            exchange.session = None
            await asyncio.wait_for(exchange.close(), timeout=0.00002)
        except:
            pass


# =====================================================================
# 🌟 对外暴露的公共 API [严格未修改 & 新增资金费率 API & 新增 OI API]
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

    【跨进程单飞去重 (Single-Flight)】
      同机多进程发起「完全同参」请求时，只有一个进程真实打网络（Leader），
      其余进程复用其结果（Follower），把网络权重与 IO 压力从 N 降到 1。

    :param dedupe: 是否启用跨进程去重（False = 100% 退回旧版行为，可用于灰度/紧急回滚）
    :param cache_dir: 缓存根目录，快照落在 {cache_dir}/_snapshots，锁落在 {cache_dir}/_locks
    :param lock_timeout: 排队等锁上限秒数。None = 自动按「目标收盘 + 60s 硬熔断 + 180s 余量」推算
    :param snapshot_ttl_sec: 完整快照的有效期（None = 永久，因为历史 K 线是幂等的）
    :param incomplete_snapshot_ttl_sec: 「有断缺」快照的宽限复用期，用于吸收瞬时并发风暴
    :param snapshot_gc_keep_sec: 快照磁盘保留时长，超期由后台 GC 清理
    """
    # ---------- 0. 事件循环护栏（与旧版一致） ----------
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        raise RuntimeError("检测到已存在运行中的异步事件循环。\n请在顶部执行：import nest_asyncio; nest_asyncio.apply()")

    def _run_core():
        return asyncio.run(
            _async_core_sniping_orchestrator(
                symbol_list, timeframe, days, target_time_str, use_ws, use_rest, proxy_url
            )
        )

    if not symbol_list:
        return {}
    if not dedupe:
        return _run_core()

    # ---------- 1. 构建请求指纹（失败则静默降级，绝不因去重逻辑导致业务失败） ----------
    try:
        sig = _build_kline_request_signature(symbol_list, timeframe, days, target_time_str)
    except Exception as e:
        logger.warning(f"[DEDUPE] ⚠️ 请求指纹构建失败({e})，本次退化为独立拉取")
        return _run_core()

    snapshot_dir = os.path.join(cache_dir, _SNAPSHOT_DIRNAME)
    lock_dir = os.path.join(cache_dir, _LOCK_DIRNAME)
    try:
        os.makedirs(snapshot_dir, exist_ok=True)
        os.makedirs(lock_dir, exist_ok=True)
    except Exception as e:
        logger.warning(f"[DEDUPE] ⚠️ 去重目录创建失败({e})，本次退化为独立拉取")
        return _run_core()

    log_prefix = f"[DEDUPE|{sig['key'][-17:]}|PID{os.getpid()}]"

    def _remap(payload):
        """快照按归一化后的 symbol 存储，返回时映射回调用方传入的原始 key，保证外部无感"""
        try:
            return {sym: payload[str(sym).strip()] for sym in symbol_list}
        except KeyError:
            return None

    # ---------- 2. L1 无锁快路径：快照直出，零网络请求 ----------
    hit = _read_kline_snapshot(sig, snapshot_dir, snapshot_ttl_sec, incomplete_snapshot_ttl_sec, log_prefix)
    hit = _remap(hit) if hit is not None else None
    if hit is not None:
        logger.info(f"{log_prefix} ⚡ L1 快照直出 | symbols={len(hit)} rows={sum(len(v) for v in hit.values())} 网络请求=0")
        return hit

    # ---------- 3. L2 抢跨进程锁 + 双重检查锁定 ----------
    if lock_timeout is None:
        now_ms = time.time() * 1000
        # 基础等待：目标收盘时间 + 60s
        base_wait = (sig['target_close_time_ms'] + 60_000 - now_ms) / 1000.0
        # 历史拉取补偿：假设每拉取1天的数据，允许额外多等 2 秒钟的网络I/O时间
        history_buffer = float(sig['days']) * 2.0
        # 综合计算：保底 180 秒，如果是过去的历史数据，以 (180 + 补偿) 为准
        lock_timeout = max(180.0, base_wait + history_buffer + 180.0)

    mutex = InterProcessMutex(os.path.join(lock_dir, f"{sig['key']}.lock"))
    wait_t0 = time.time()

    def _on_wait(waited):
        logger.info(f"{log_prefix} ⏳ 同参进程正在拉取，本进程排队等待 | waited={waited:.0f}s / limit={lock_timeout:.0f}s")

    if not mutex.acquire(timeout=lock_timeout, poll_interval=0.2, on_wait=_on_wait, wait_log_interval=45.0):
        logger.warning(f"{log_prefix} ⚠️ 等锁超时({lock_timeout:.0f}s)，为保障可用性降级为独立拉取（可能出现重复请求）")
        return _run_core()

    try:
        # 双重检查：等锁期间 Leader 极可能已交付
        hit = _read_kline_snapshot(sig, snapshot_dir, snapshot_ttl_sec, incomplete_snapshot_ttl_sec, log_prefix)
        hit = _remap(hit) if hit is not None else None
        if hit is not None:
            logger.info(
                f"{log_prefix} ✅ L2 双重检查命中（复用 Leader 成果） | wait={time.time() - wait_t0:.2f}s "
                f"rows={sum(len(v) for v in hit.values())} 网络请求=0")
            return hit

        # ---------- 4. 当选 Leader：真实拉取 ----------
        logger.info(f"{log_prefix} 👑 当选 Leader，开始真实拉取 | wait={time.time() - wait_t0:.2f}s "
                    f"target={_format_bj_time(sig['target_time_ms'])} symbols={len(sig['symbols'])}")
        result = _run_core()

        # 交付前同步写快照（Follower 是在锁释放后才做双重检查的，故不能异步写）
        try:
            _write_kline_snapshot(sig, result, snapshot_dir, log_prefix)
        except Exception as e:
            logger.error(f"{log_prefix} ❌ 快照写入异常（不影响本次返回）: {e}")

        return result

    finally:
        try:
            mutex.release()
        except Exception as e:
            logger.warning(f"{log_prefix} ⚠️ 锁释放异常(OS 将在进程退出时自动回收): {e}")
        _maybe_dispatch_dedupe_gc(snapshot_dir, lock_dir, snapshot_gc_keep_sec, log_prefix)

def snipe_funding_rate_data(symbol_list, days, proxy_url=None):
    """
    🚀 同步入口：极速获取带有本地缓存和临近结算脉冲探测的最新资金费率数据。
    :param days: 期望获取的历史天数 (从当前时间往前推)
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        raise RuntimeError("检测到已存在运行中的异步事件循环。\n请在顶部执行：import nest_asyncio; nest_asyncio.apply()")

    return asyncio.run(
        _async_core_funding_orchestrator(
            symbol_list, days, proxy_url
        )
    )


def snipe_oi_data(symbol_list, timeframe, days, target_time_str, proxy_url=None):
    """
    🚀 同步入口：极速获取 Open Interest (未平仓合约) 历史数据。
    纯REST全链路轮询拉取，指定截止时间，确保返回 O(1) [timestamp, oi_amount] 双列极简切片。
    :param symbol_list: 币种列表
    :param timeframe: OI 获取的周期，如 '5m', '1h'
    :param days: 期望获取的历史天数
    :param target_time_str: 期望获取的截止时间边界
    """
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        raise RuntimeError("检测到已存在运行中的异步事件循环。\n请在顶部执行：import nest_asyncio; nest_asyncio.apply()")

    return asyncio.run(
        _async_core_oi_orchestrator(
            symbol_list, timeframe, days, target_time_str, proxy_url
        )
    )


# =====================================================================
# 🚀 启动入口 [严格未修改（仅增加了 OI 的调用示例）]
# =====================================================================
if __name__ == "__main__":

    while True:
        symbol_list = [
            "BTC/USDC:USDC"
        ]

        target_time = (datetime.now() + timedelta(minutes=0)).strftime("%Y-%m-%d %H:%M")

        logger.info(">>> 准备调用数据引擎...")

        # # ======= 1. K线数据极速引擎调用演示 =======
        # result_map = snipe_kline_data(
        #     symbol_list=symbol_list,
        #     timeframe="15m",
        #     days=10,
        #     target_time_str=target_time,
        #     use_ws=True,
        #     use_rest=True,
        #     proxy_url='http://127.0.0.1:7890'
        # )
        # logger.info(f"✅ 已完成对所有币种的极速K线引擎数据请求，正在进行数据完整性检查和预处理...")

        # ======= 2. 资金费率极速引擎调用演示 =======
        # logger.info(">>> 准备调用资金费率极速引擎...")
        # funding_result_map = snipe_funding_rate_data(
        #     symbol_list=symbol_list,
        #     days=150,
        #     proxy_url='http://127.0.0.1:7890'
        # )
        # logger.info(f"✅ 已完成资金费率数据请求，返回了指定区间的去重结算数据。")

        # ======= 3. OI 未平仓合约极速引擎调用演示 =======
        logger.info(">>> 准备调用 OI 未平仓合约极速引擎...")
        oi_result_map = snipe_oi_data(
            symbol_list=symbol_list,
            timeframe="5m",
            days=20,
            target_time_str=target_time,  # 加入指定的统一时间边界
            proxy_url='http://127.0.0.1:7890'
        )
        logger.info(f"✅ 已完成 OI 数据纯REST请求，自动合并落盘并返回两列 O(1) 切片。")
        # print(oi_result_map['BTC/USDC:USDC'].head()) # 打印验证，应仅包含 timestamp 与 oi_amount

        break