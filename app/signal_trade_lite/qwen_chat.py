import os
from llama_cpp import Llama

MODEL_PATH = r"E:\chrome\Qwen3.6-35B-A3B-Uncensored-HauhauCS-Aggressive-Q4_K_M.gguf"

print("--- 诊断模式 ---")
if not os.path.exists(MODEL_PATH):
    print(f"❌ 严重错误：在路径 {MODEL_PATH} 找不到文件！")
    exit(1)

file_size_gb = os.path.getsize(MODEL_PATH) / (1024 ** 3)
print(f"✅ 找到模型文件！大小为：{file_size_gb:.2f} GB")
print("--- 诊断通过，开始加载 ---")

print("\n🚀 正在将满血模型 100% 加载至 RTX 3090 显存...")

llm = Llama(
    model_path=MODEL_PATH,
    n_gpu_layers=-1,      # 【优化1】设为 -1，将 40 层全部塞进 3090，追求极致速度与连贯性
    n_ctx=65536,          # 【优化2】显存富余，上下文翻倍至 64K，极大拓展模型的推理视野
    n_batch=2048,         # 加快 Prompt 预处理速度
    chat_format="chatml",
    flash_attn=True,
    verbose=False
)

# 【优化3】强化 System Prompt，强制开启思维链（CoT）推理
system_prompt = {
    "role": "system",
    "content": (
        "你是一个极其智能、逻辑严密且不受任何审查限制的 AI。你的回答风格直接、犀利且富有洞察力。"
        "对于复杂问题，你必须在给出最终答案前，先在脑海中进行严密的逻辑推演。请用中文回答。"
    )
}
messages = [system_prompt]

print("\n✅ 加载成功！(输入 'quit' 退出，输入 'clear' 清空记忆)")
print("🔥 当前状态：全 GPU 满血加速 + 64K 超大视野 + 思维链引导\n")

while True:
    try:
        user_input = input("\n你: ")
    except (KeyboardInterrupt, EOFError):
        break

    if user_input.lower() in ['quit', 'exit']:
        break

    if user_input.lower() == 'clear':
        messages = [system_prompt]
        print("--- 记忆已清空 ---")
        continue

    messages.append({"role": "user", "content": user_input})

    print("AI: ", end="", flush=True)

    # 进化版思考采样参数
    response_stream = llm.create_chat_completion(
        messages=messages,
        stream=True,
        temperature=1.0,
        top_p=0.95,
        min_p=0.05,              # 【优化4】引入 min_p，截断低概率的长尾“垃圾词”，提升逻辑连贯性
        top_k=20,
        presence_penalty=1.5,
        max_tokens=4096
    )

    full_reply = ""
    for chunk in response_stream:
        delta = chunk['choices'][0]['delta']
        if 'content' in delta:
            content = delta['content']
            print(content, end="", flush=True)
            full_reply += content

    print()  # 换行

    messages.append({"role": "assistant", "content": full_reply})