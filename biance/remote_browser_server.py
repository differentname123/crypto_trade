# -*- coding: utf-8 -*-
import os
import time
import threading
from urllib.parse import unquote
from flask import Flask, request, Response, jsonify
from playwright.sync_api import sync_playwright

# 借用你原有代码的配置和清理函数
USER_DATA_DIR = r"W:\temp\biance_yang"
BINANCE_URL = "https://www.binance.com/zh-CN/login"


def clean_browser_cache(user_data_dir):
    import shutil
    if not os.path.exists(user_data_dir): return
    garbage = ("Cache", "Code Cache", "GPUCache", "ShaderCache", "Service Worker")
    for base in (user_data_dir, os.path.join(user_data_dir, "Default")):
        for name in garbage:
            path = os.path.join(base, name)
            if os.path.exists(path):
                try:
                    shutil.rmtree(path, ignore_errors=True) if os.path.isdir(path) else os.remove(path)
                except:
                    pass


# ==============================================================================
#                      全局状态与 Playwright 控制器
# ==============================================================================

class BrowserSession:
    def __init__(self):
        self.playwright = None
        self.context = None
        self.page = None
        self.is_running = False
        self.lock = threading.Lock()
        self.last_frame = None

    def start(self, user_data_dir, url):
        with self.lock:
            if self.is_running:
                return False, "浏览器已经在运行中"

            clean_browser_cache(user_data_dir)
            self.is_running = True

        # 在独立线程中启动 Playwright，防止阻塞 Flask 主线程
        threading.Thread(target=self._run_browser_thread, args=(user_data_dir, url), daemon=True).start()
        return True, "浏览器已启动"

    def _run_browser_thread(self, user_data_dir, url):
        try:
            self.playwright = sync_playwright().start()

            # 使用与你原有代码相同的隐蔽参数启动
            anti_freeze_args = ['--disable-restore-session-state', '--no-default-browser-check']
            offscreen_args = [
                                 '--disable-blink-features=AutomationControlled',
                                 '--disable-gpu',
                                 '--no-sandbox',
                                 '--disable-dev-shm-usage',
                                 '--hide-scrollbars',
                             ] + anti_freeze_args

            self.context = self.playwright.chromium.launch_persistent_context(
                user_data_dir=user_data_dir,
                headless=False,  # 必须 Headed 才能应对某些极端的 CF 验证
                viewport={'width': 1280, 'height': 800},
                args=offscreen_args,
                ignore_default_args=["--enable-automation"]
            )

            self.page = self.context.pages[0] if self.context.pages else self.context.new_page()
            self.page.goto(url, timeout=60000)

            # 保持线程存活，持续更新截图
            while self.is_running:
                try:
                    if self.page.is_closed():
                        break
                    # 每秒抓取一次画面缓存 (质量降低以保证流畅度)
                    self.last_frame = self.page.screenshot(type="jpeg", quality=40)
                    time.sleep(0.5)
                except Exception as e:
                    time.sleep(1)

        except Exception as e:
            print(f"[远程浏览器] 运行异常: {e}")
        finally:
            self.stop()

    def stop(self):
        with self.lock:
            self.is_running = False
            self.last_frame = None
            try:
                if self.context:
                    self.context.close()
            except:
                pass
            try:
                if self.playwright:
                    self.playwright.stop()
            except:
                pass
            self.context = None
            self.page = None
            self.playwright = None
            print("[远程浏览器] 会话已断开，Cookie 已保存，资源已释放。")

    def click(self, x, y):
        if self.is_running and self.page and not self.page.is_closed():
            try:
                self.page.mouse.click(x, y)
                return True
            except Exception as e:
                print(f"[远程浏览器] 点击失败: {e}")
        return False

    def type_text(self, text):
        """支持从手机端发送文字到焦点输入框"""
        if self.is_running and self.page and not self.page.is_closed():
            try:
                self.page.keyboard.insert_text(text)
                return True
            except Exception:
                pass
        return False


# 实例化全局单例
browser_session = BrowserSession()

# ==============================================================================
#                             Flask Web 服务
# ==============================================================================

app = Flask(__name__)

# 极简且适配手机的 HTML 遥控器界面
HTML_TEMPLATE = """
<!DOCTYPE html>
<html>
<head>
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>币安远程接管终端</title>
    <style>
        body { margin: 0; padding: 0; background: #1E2329; color: white; font-family: -apple-system, sans-serif; }
        .header { padding: 15px; background: #0B0E11; text-align: center; border-bottom: 1px solid #333; }
        .controls { display: flex; gap: 10px; padding: 15px; justify-content: center; }
        button { padding: 10px 15px; font-size: 14px; border: none; border-radius: 4px; font-weight: bold; cursor: pointer; flex: 1; }
        .btn-start { background: #FCD535; color: #1E2329; }
        .btn-stop { background: #F6465D; color: white; }
        .btn-input { background: #2B3139; color: white; }
        #screen-container { position: relative; width: 100%; text-align: center; background: #000; min-height: 200px; display: flex; align-items: center; justify-content: center; }
        #screen { max-width: 100%; height: auto; touch-action: none; display: none; }
        #placeholder { color: #848E9C; font-size: 14px; }
        .toast { position: fixed; top: 20px; left: 50%; transform: translateX(-50%); background: rgba(0,0,0,0.8); color: white; padding: 8px 16px; border-radius: 20px; font-size: 12px; display: none; z-index: 100; pointer-events: none;}
    </style>
</head>
<body>
    <div class="toast" id="toast"></div>
    <div class="header">
        <h3 style="margin:0">币安环境初始化中心</h3>
        <div style="font-size:12px; color:#848E9C; margin-top:5px;">USER_DATA_DIR 状态管理</div>
    </div>

    <div class="controls">
        <button class="btn-start" onclick="startSession()">▶ 启动浏览器</button>
        <button class="btn-input" onclick="sendInput()">⌨ 键入文字</button>
        <button class="btn-stop" onclick="stopSession()">⏹ 断开连接</button>
    </div>

    <div id="screen-container">
        <div id="placeholder">浏览器未运行，请点击启动</div>
        <img id="screen" src="" alt="Screen">
    </div>

    <script>
        const img = document.getElementById('screen');
        const placeholder = document.getElementById('placeholder');
        let refreshInterval = null;

        function showToast(msg) {
            const t = document.getElementById('toast');
            t.innerText = msg;
            t.style.display = 'block';
            setTimeout(() => t.style.display = 'none', 2000);
        }

        function startImageRefresh() {
            img.style.display = 'inline-block';
            placeholder.style.display = 'none';
            if(refreshInterval) clearInterval(refreshInterval);

            // 每秒获取最新画面
            refreshInterval = setInterval(() => {
                img.src = '/api/screenshot?t=' + new Date().getTime();
            }, 800);
        }

        function stopImageRefresh() {
            if(refreshInterval) clearInterval(refreshInterval);
            img.style.display = 'none';
            placeholder.style.display = 'block';
            img.src = "";
        }

        function startSession() {
            showToast("正在启动浏览器...");
            fetch('/api/start', {method: 'POST'})
                .then(r => r.json())
                .then(d => {
                    showToast(d.msg);
                    if(d.status === 'ok') startImageRefresh();
                });
        }

        function stopSession() {
            fetch('/api/stop', {method: 'POST'})
                .then(r => r.json())
                .then(d => {
                    showToast(d.msg);
                    stopImageRefresh();
                });
        }

        function sendInput() {
            const text = prompt("请输入要发送到网页的文字（请先点击网页上的输入框让其获得焦点）:");
            if(text) {
                fetch('/api/type', {
                    method: 'POST',
                    headers: {'Content-Type': 'application/json'},
                    body: JSON.stringify({text: text})
                }).then(() => showToast("已发送文字"));
            }
        }

        // 监听点击事件，计算相对坐标
        img.addEventListener('click', function(e) {
            const rect = img.getBoundingClientRect();
            // 计算真实浏览器内的坐标映射
            const scaleX = img.naturalWidth / rect.width;
            const scaleY = img.naturalHeight / rect.height;
            const realX = Math.round((e.clientX - rect.left) * scaleX);
            const realY = Math.round((e.clientY - rect.top) * scaleY);

            fetch(`/api/click?x=${realX}&y=${realY}`);

            // 点击视觉反馈
            const marker = document.createElement('div');
            marker.style.position = 'absolute';
            marker.style.width = '10px';
            marker.style.height = '10px';
            marker.style.background = 'rgba(252, 213, 53, 0.8)'; // 币安黄
            marker.style.borderRadius = '50%';
            marker.style.left = (e.clientX - rect.left - 5) + 'px';
            marker.style.top = (e.clientY - rect.top - 5) + 'px';
            marker.style.pointerEvents = 'none';
            document.getElementById('screen-container').appendChild(marker);
            setTimeout(() => marker.remove(), 500);
        });

        // 页面关闭时自动通知后端断开，防止资源泄漏
        window.addEventListener('beforeunload', () => {
            navigator.sendBeacon('/api/stop');
        });
    </script>
</body>
</html>
"""


@app.route('/')
def index():
    return HTML_TEMPLATE


@app.route('/api/start', methods=['POST'])
def api_start():
    success, msg = browser_session.start(USER_DATA_DIR, BINANCE_URL)
    return jsonify({"status": "ok" if success else "error", "msg": msg})


@app.route('/api/stop', methods=['POST'])
def api_stop():
    browser_session.stop()
    return jsonify({"status": "ok", "msg": "已断开连接并保存数据"})


@app.route('/api/screenshot')
def api_screenshot():
    if browser_session.is_running and browser_session.last_frame:
        return Response(browser_session.last_frame, mimetype='image/jpeg')
    return "No frame", 404


@app.route('/api/click')
def api_click():
    x = float(request.args.get('x', 0))
    y = float(request.args.get('y', 0))
    browser_session.click(x, y)
    return "ok"


@app.route('/api/type', methods=['POST'])
def api_type():
    data = request.json
    if data and 'text' in data:
        browser_session.type_text(data['text'])
    return "ok"


if __name__ == '__main__':
    print(f"🚀 手机远程管理终端已启动！")
    print(f"👉 请配置 ngrok 映射到 5000 端口: ngrok http 5000")
    print(f"👉 使用手机浏览器访问 ngrok 提供的 HTTPS 地址")
    # debug 必须为 False，否则 Flask 重载会引发 Playwright 线程冲突
    app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)