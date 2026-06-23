from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
import httpx
import os

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