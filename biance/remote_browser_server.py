# -*- coding: utf-8 -*-
import os
import time
import threading
import queue
from flask import Flask, render_template_string, Response, request, jsonify
from playwright.sync_api import sync_playwright

from common.common_utils import read_json

# ==============================================================================
#                                   运行配置与环境
# ==============================================================================
HOME_URL = "https://www.binance.com/zh-CN/square"
VIEWPORT_WIDTH = 1920
VIEWPORT_HEIGHT = 1080

# 复用你原有的启动参数，确保环境绝对一致
ANTI_FREEZE_ARGS = ['--disable-restore-session-state', '--no-default-browser-check']
OFFSCREEN_ARGS = [
                     '--disable-blink-features=AutomationControlled', '--disable-gpu',
                     '--window-position=0,0', '--no-sandbox', '--disable-dev-shm-usage',
                     '--disable-renderer-backgrounding', '--disable-background-timer-throttling',
                     '--disable-backgrounding-occluded-windows', '--disable-features=CalculateNativeWinOcclusion',
                     '--disable-breakpad', '--force-device-scale-factor=1', '--hide-scrollbars',
                 ] + ANTI_FREEZE_ARGS


# ==============================================================================
#                            核心：Playwright 后台引擎
# ==============================================================================
class BrowserEngine:
    def __init__(self):
        self.is_running = False
        self.thread = None
        self.cmd_queue = queue.Queue()
        self.current_frame = None  # 存放最新一帧截图的二进制流
        self.user_data_dir = None  # 存放当前使用的动态目录

    def start(self, user_data_dir):
        if self.is_running:
            return False
        self.user_data_dir = user_data_dir
        self.is_running = True
        self.thread = threading.Thread(target=self._run_browser_loop, daemon=True)
        self.thread.start()
        return True

    def stop(self):
        if not self.is_running:
            return False
        # 发送停止指令到队列
        self.cmd_queue.put({"type": "stop"})
        if self.thread:
            self.thread.join(timeout=5)
        return True

    def click(self, ratio_x, ratio_y):
        if self.is_running:
            self.cmd_queue.put({"type": "click", "rx": ratio_x, "ry": ratio_y})

    def _run_browser_loop(self):
        """完全独立的单线程，负责维持 Playwright 运转、接收指令和不断截图"""
        print(f"[引擎] 正在挂载目录启动: {self.user_data_dir}")
        try:
            with sync_playwright() as p:
                context = p.chromium.launch_persistent_context(
                    user_data_dir=self.user_data_dir,
                    channel="chrome",
                    headless=False,
                    args=OFFSCREEN_ARGS,
                    no_viewport=True,
                    ignore_default_args=["--enable-automation"],
                    viewport={'width': VIEWPORT_WIDTH, 'height': VIEWPORT_HEIGHT}
                )

                page = context.pages[0] if context.pages else context.new_page()
                page.bring_to_front()

                try:
                    page.goto(HOME_URL, timeout=30000)
                except Exception as e:
                    print(f"[引擎] 初始导航超时或异常 (可忽略): {e}")

                print("[引擎] 浏览器已就绪，开始推流与监听控制...")

                # 主控循环
                while self.is_running:
                    # 1. 处理来自 Flask Web 的队列指令
                    try:
                        cmd = self.cmd_queue.get_nowait()
                        if cmd["type"] == "stop":
                            print("[引擎] 收到停止信号，准备安全释放进程...")
                            break
                        elif cmd["type"] == "click":
                            # 将前端传来的比例 (0.0~1.0) 还原为真实像素绝对坐标
                            abs_x = cmd["rx"] * VIEWPORT_WIDTH
                            abs_y = cmd["ry"] * VIEWPORT_HEIGHT
                            print(f"[操作] 模拟点击绝对坐标: ({abs_x:.1f}, {abs_y:.1f})")
                            page.mouse.click(abs_x, abs_y)
                    except queue.Empty:
                        pass
                    except Exception as e:
                        print(f"[操作] 指令执行异常: {e}")

                    # 2. 截取最新画面并塞入共享内存 (MJPEG)
                    try:
                        # 【优化带宽】降低质量至 15 (极高压缩率)，足以辨认按钮位置
                        frame = page.screenshot(type="jpeg", quality=15)
                        self.current_frame = frame
                    except Exception as e:
                        # 页面加载、崩溃或关闭时可能截图失败，暂避即可
                        pass

                    # 【优化带宽】控制推流帧率下降到约 5 FPS，大幅降低传输开销
                    time.sleep(0.2)

                    # 退出循环，执行安全释放
                context.close()
                print("[引擎] 上下文已关闭，文件锁已安全释放。")
        except Exception as e:
            print(f"[引擎] 崩溃: {e}")
        finally:
            self.is_running = False


# 实例化全局单例引擎
engine = BrowserEngine()

# ==============================================================================
#                                Flask Web 服务层
# ==============================================================================
app = Flask(__name__)

# 前端 HTML & JS
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="zh-CN">
<head>
    <meta charset="UTF-8">
    <title>币安自动化 - 远程接管台</title>
    <style>
        body { font-family: -apple-system, system-ui; background: #121212; color: #fff; margin: 0; padding: 20px; display: flex; flex-direction: column; align-items: center; }
        .controls { margin-bottom: 20px; display: flex; gap: 15px; align-items: center; flex-wrap: wrap; justify-content: center; }
        button { padding: 10px 20px; font-size: 16px; cursor: pointer; border: none; border-radius: 5px; font-weight: bold; }
        select { padding: 10px; font-size: 16px; border-radius: 5px; background: #222; color: #fff; border: 1px solid #555; max-width: 400px;}
        .btn-refresh { background: #3498db; color: #fff; }
        .btn-start { background: #00c087; color: #000; }
        .btn-stop { background: #f6465d; color: #fff; }
        .screen-container {
            width: 90vw;
            max-width: 1200px;
            /* 强制与浏览器实际比例 16:9 保持一致，确保坐标等比映射绝对精确 */
            aspect-ratio: 1920 / 1080;
            border: 2px solid #333;
            border-radius: 8px;
            overflow: hidden;
            background: #000;
            position: relative;
            box-shadow: 0 4px 15px rgba(0,0,0,0.5);
        }
        /* 禁止图片拖拽选中，确保纯粹的点击事件 */
        #browser-screen { width: 100%; height: 100%; object-fit: contain; cursor: crosshair; user-select: none; -webkit-user-drag: none; }
        .status { margin-top: 10px; color: #888; }
    </style>
</head>
<body>
    <h2>🚀 Binance Playwright 远程接管台</h2>

    <div class="controls">
        <select id="accountSelect">
            <option value="">加载中...</option>
        </select>
        <button class="btn-refresh" onclick="refreshAccounts()">🔄 刷新列表</button>
        <button class="btn-start" onclick="startEngine()">▶ 启动 / 连接</button>
        <button class="btn-stop" onclick="sendCommand('/stop')">⏹ 安全断开</button>
    </div>

    <div class="screen-container">
        <!-- MJPEG 流式传输源 -->
        <img id="browser-screen" src="" alt="等待浏览器启动..." />
    </div>
    <div class="status" id="log">状态: 未连接</div>

    <script>
        // 初始化拉取账号列表
        window.onload = refreshAccounts;

        function refreshAccounts() {
            document.getElementById('log').innerText = '正在实时拉取错误账号列表...';
            fetch('/accounts')
                .then(r => r.json())
                .then(res => {
                    if(res.status === 'ok') {
                        const sel = document.getElementById('accountSelect');
                        sel.innerHTML = '';
                        if (res.data.length === 0) {
                            sel.innerHTML = '<option value="">(列表为空)</option>';
                        } else {
                            res.data.forEach(dir => {
                                const opt = document.createElement('option');
                                opt.value = dir;
                                opt.textContent = dir;
                                sel.appendChild(opt);
                            });
                        }
                        document.getElementById('log').innerText = '账号列表刷新成功 (共 ' + res.data.length + ' 个)';
                    } else {
                        document.getElementById('log').innerText = '拉取列表失败: ' + res.msg;
                    }
                });
        }

        function sendCommand(endpoint) {
            document.getElementById('log').innerText = '正在发送指令...';
            fetch(endpoint)
                .then(r => r.json())
                .then(data => {
                    document.getElementById('log').innerText = data.msg;
                });
        }

        function startEngine() {
            const sel = document.getElementById('accountSelect');
            if (!sel.value) {
                alert("请先选择一个有效的账号目录！");
                return;
            }

            document.getElementById('log').innerText = '正在请求启动: ' + sel.value;
            fetch('/start', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({ user_data_dir: sel.value })
            })
            .then(r => r.json())
            .then(data => {
                document.getElementById('log').innerText = data.msg;
                if (data.status === 'ok') {
                    // 延迟刷新视频流，留给 Playwright 1秒启动时间
                    setTimeout(() => {
                        document.getElementById('browser-screen').src = "/video_feed?" + new Date().getTime();
                    }, 1000);
                }
            });
        }

        // 核心：处理画面点击并按比例映射坐标
        const screenEl = document.getElementById('browser-screen');
        screenEl.addEventListener('mousedown', function(e) {
            if (!this.src || this.src.endsWith('undefined')) return;

            // 获取当前 img 标签在页面上的实际物理大小和位置
            const rect = this.getBoundingClientRect();

            // 计算点击位置在画面中的比例 (0.00 ~ 1.00)
            const ratioX = (e.clientX - rect.left) / rect.width;
            const ratioY = (e.clientY - rect.top) / rect.height;

            // 过滤掉点在黑边上的无效操作
            if (ratioX < 0 || ratioX > 1 || ratioY < 0 || ratioY > 1) return;

            document.getElementById('log').innerText = `发送点击: [${(ratioX*100).toFixed(1)}%, ${(ratioY*100).toFixed(1)}%]`;

            fetch('/click', {
                method: 'POST',
                headers: {'Content-Type': 'application/json'},
                body: JSON.stringify({rx: ratioX, ry: ratioY})
            });
        });
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    return render_template_string(HTML_TEMPLATE)


@app.route('/accounts', methods=['GET'])
def get_accounts():
    """实时读取 error_dir_list.json 返回给前端"""
    try:
        dir_list = read_json("error_user_data_dir.json")
        if not isinstance(dir_list, list):
            dir_list = []
        return jsonify({"status": "ok", "data": dir_list})
    except Exception as e:
        return jsonify({"status": "error", "msg": str(e)})


@app.route('/start', methods=['POST'])
def start_browser():
    """接收前端传来的具体目录，挂载并启动浏览器"""
    data = request.json
    user_data_dir = data.get("user_data_dir")

    if not user_data_dir:
        return jsonify({"status": "error", "msg": "未提供账号目录"})

    success = engine.start(user_data_dir)
    return jsonify({"status": "ok", "msg": "浏览器启动成功" if success else "浏览器已经在运行中，请先停止当前运行账号"})


@app.route('/stop')
def stop_browser():
    success = engine.stop()
    return jsonify({"status": "ok", "msg": "正在安全释放并关闭" if success else "浏览器未运行"})


@app.route('/click', methods=['POST'])
def handle_click():
    data = request.json
    engine.click(data.get('rx'), data.get('ry'))
    return jsonify({"status": "ok"})


def mjpeg_generator():
    """将 Playwright 的 JPEG 截图打包为 HTTP 混合替换流 (MJPEG)"""
    while True:
        if engine.is_running and engine.current_frame:
            yield (b'--frame\r\n'
                   b'Content-Type: image/jpeg\r\n\r\n' + engine.current_frame + b'\r\n')
        else:
            # 浏览器未启动时，发送空流防止前端报错，降低休眠时间
            time.sleep(0.5)
        time.sleep(0.05)


@app.route('/video_feed')
def video_feed():
    return Response(mjpeg_generator(), mimetype='multipart/x-mixed-replace; boundary=frame')


# ==============================================================================
#                                    启动 Web
# ==============================================================================
if __name__ == '__main__':
    print("===================================================")
    print(" 🕸️ Web 控制台启动成功！")
    print(" 🔗 请在浏览器中打开: http://127.0.0.1:5000")
    print("===================================================")
    # 不使用 reloader，防止后台线程被创建多次
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)