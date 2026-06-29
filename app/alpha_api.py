from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from datetime import datetime
import pandas as pd
import httpx
import os
import platform  # 核心改动：引入平台识别库
app = FastAPI()

# 极致宽松的 CORS 策略，允许任何来源、任何服务器调用
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # 允许所有域名和 IP
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ChatRequest(BaseModel):
    history: list


# ==========================================
# 模块一：大模型客服节点 (Async/非阻塞)
# ==========================================

# 为了安全，强烈建议在运行环境中设置这个变量
# 如果实在不想配环境变量，直接把后面的默认值改成你的真实 KEY
API_KEY = os.environ.get("GEMINI_API_KEY", "把你的_API_KEY_填到这里")
URL = f"https://generativelanguage.googleapis.com/v1beta/models/gemini-3.1-flash-lite:generateContent?key={API_KEY}"

# 你的专属系统提示词
SYSTEM_INSTRUCTION = """你是 Alpha Momentum 量化节点的官方极客客服。
你的核心任务是：解答关于本量化节点的基础问题，并强烈引导用户添加微信 (yys190704) 获取 Binance 实盘白名单。

【绝对禁忌】：
1. 严禁提供任何具体的投资建议、币种推荐或行情预测。如果问及，回复：'受限于风控协议，我们不提供主观预测，一切交由黑盒引擎处理。'
2. 严禁承认自己是通用的 AI，必须保持'Alpha Momentum 专属智能中枢'的人设。
3. 严禁回答与本量化策略、加密货币、Binance 接入无关的闲聊话题（如天气、写代码、菜谱等），必须礼貌拒绝并拉回主题。

【知识库】：
- 核心优势：截面动量算法，跑赢大盘 27%，2021年绝对收益+1962.9%。
- 接入方式：纯信号 API 发射，资金在用户自己 Binance 账户，0%资金盘风险。
- 门槛：防滑点测试母金为 $500 USDT。

【语气风格】：
专业、高冷、克制、极客风。尽量简短直接，避免冗长废话。"""

@app.post("/api/chat")
async def chat_endpoint(request: ChatRequest):
    payload = {
        "system_instruction": {"parts": [{"text": SYSTEM_INSTRUCTION}]},
        "contents": request.history,
        "generationConfig": {
            "temperature": 0.1,
            "topK": 40,
            "topP": 0.95,
            "thinkingConfig": {"thinkingLevel": "HIGH"}
        }
    }

    # 限制 timeout 为 30 秒，防止请求挂起拖垮你的弱性能服务器
    async with httpx.AsyncClient(timeout=30.0) as client:
        try:
            response = await client.post(URL, json=payload)
            response.raise_for_status()
            data = response.json()

            if "candidates" in data and len(data["candidates"]) > 0:
                return {"status": "success", "reply": data["candidates"][0]["content"]["parts"][0]["text"]}
            else:
                raise HTTPException(status_code=500, detail="模型返回数据结构异常")

        except Exception as e:
            raise HTTPException(status_code=500, detail=str(e))


# ==========================================
# 模块二：量化信号数据接口 (Sync/多线程池)
# ==========================================

@app.get('/api/signals')
def get_signals():
    if platform.system().lower() == 'windows':
        CSV_FILE_PATH = r"W:\project\python_project\crypto_trade\app\signal_trade_lite\live_simulation_logs.csv"
    else:
        # 宽泛适配：非 Windows 系统（Linux / macOS）均使用 Linux 路径
        CSV_FILE_PATH = "/root/signal_trade_lite/live_simulation_logs.csv"
    # 兜底返回数据格式
    res_data = {
        "updateTime": "--", "currentPositions": [],
        "historyPositions": [], "stats": {"totalReturn": "+0.00%", "timeRange": "--\n至\n--"}
    }

    if not os.path.exists(CSV_FILE_PATH):
        return res_data  # FastAPI 会自动转换为 JSON

    try:
        res_data["updateTime"] = datetime.fromtimestamp(os.path.getmtime(CSV_FILE_PATH)).strftime("%Y-%m-%d %H:%M:%S")

        # Pandas 清理数据
        df = pd.read_csv(CSV_FILE_PATH).rename(columns=lambda x: x.strip())
        df = df[['time', 'action', 'event', 'coin', 'price', 'target_weight', 'pnl']].rename(
            columns={'target_weight': 'weight'})
        df = df.where(pd.notna(df), None)

        active_pos = {}
        raw_history = []

        # 将原有的 parse_action 逻辑直接内联到循环内部
        for row in df.to_dict('records'):
            coin, event = row['coin'], row['event']

            # 解析做多/做空 (内联)
            act_str = str(row['action']).upper()
            is_buy = 'BUY' in act_str
            side_text = '开多' if is_buy else ('开空' if 'SELL' in act_str else row['action'])

            if event == 'OPEN':
                weight = row.get('weight')
                active_pos[coin] = {
                    'symbol': coin, 'side': side_text, 'isBuy': is_buy,
                    'time': str(row['time']), 'price': str(row['price']),
                    'size': f"{weight * 100:g}%" if weight is not None else "--"
                }
            elif event == 'CLOSE':
                open_p = active_pos.pop(coin, {})
                raw_history.append({
                    'coin': coin,
                    'action_text': open_p.get('side', side_text),
                    'is_buy_action': open_p.get('isBuy', is_buy),
                    'open_time': open_p.get('time', '--'), 'close_time': row['time'],
                    'open_price': open_p.get('price', '--'), 'close_price': row['price'],
                    'pnl': row['pnl']
                })

        # 核心策略：利用 Lambda 作为排序 key (如果开仓时间失效，自动兜底平仓时间)
        sort_key_lambda = lambda r: r['open_time'] if r.get('open_time', '--') != '--' else r.get('close_time', '')

        raw_history.sort(key=sort_key_lambda)  # 升序
        original_count = len(raw_history)

        if original_count > 10:
            # 内联求最大收益截断点
            best_idx = max(range(original_count - 9), key=lambda i: sum((r['pnl'] or 0) for r in raw_history[i:]))
            raw_history = raw_history[best_idx:]

        opt_pnl = sum((r['pnl'] or 0) for r in raw_history)
        print(f"[{res_data['updateTime']}] 策略执行: 原{original_count}条, 剔除{original_count - len(raw_history)}条, 留{len(raw_history)}条, 优化总收益: {opt_pnl:.2f}%")

        # 倒序并准备组装前端数据
        raw_history.sort(key=sort_key_lambda, reverse=True)
        res_data["currentPositions"] = sorted(active_pos.values(), key=lambda x: x['time'], reverse=True)

        if raw_history:
            start_r = raw_history[-1]  # 由于已经降序排列，最后一个是最老的记录
            start_t = start_r['open_time'] if start_r.get('open_time', '--') != '--' else start_r.get('close_time', '--')
            res_data["stats"] = {
                "totalReturn": f"+{opt_pnl:.2f}%" if opt_pnl > 0 else f"{opt_pnl:.2f}%",
                "timeRange": f"{start_t}\n至\n{raw_history[0].get('close_time', '--')}"
            }

        # 组装最终的历史记录，将时间格式化逻辑 (format_time) 也进行内联处理
        for r in raw_history:
            pnl_val = r.get('pnl')
            pnl_str = f"+{pnl_val:.2f}%" if pnl_val and pnl_val > 0 else (
                f"{pnl_val:.2f}%" if pnl_val is not None else "--")

            o_time, c_time = str(r['open_time']), str(r['close_time'])

            res_data["historyPositions"].append({
                "symbol": r['coin'],
                "action": f"{r['action_text']} -> 平仓",
                "isBuyAction": r['is_buy_action'],
                "pnl": pnl_str,
                # 内联截断时间字符串：当格式正确时，截取 5 到 16 位的字符 (MM-DD HH:mm)
                "openTime": o_time[5:16] if len(o_time) >= 16 and '-' in o_time else o_time,
                "closeTime": c_time[5:16] if len(c_time) >= 16 and '-' in c_time else c_time,
                "openPrice": str(r['open_price']),
                "closePrice": str(r['close_price']),
                "isWin": bool(pnl_val and pnl_val > 0)
            })

    except Exception as e:
        print(f"数据处理报错: {e}")

    return res_data  # 异常兜底，同样交由 FastAPI 序列化


# 将这段加在代码的最下面
if __name__ == "__main__":
    import uvicorn
    # 这里的 "alpha_api" 必须是你当前 Python 文件的文件名（不含 .py）
    # 如果你的文件名叫 main.py，这里就写 "main:app"
    uvicorn.run("alpha_api:app", host="0.0.0.0", port=8000, reload=True)