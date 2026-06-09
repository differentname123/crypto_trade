from flask import Flask, jsonify, render_template
import pandas as pd
import os
import math
from datetime import datetime

# 假设你的 fetch_new_df 已经导入
from app.crypto_dashboard.run_cross_signal import fetch_new_df

app = Flask(__name__)

CSV_FILE_PATH = r"W:\project\python_project\crypto_trade\app\crypto_dashboard\live_simulation_logs.csv"


@app.route('/')
def index():
    return render_template('index.html')


# 接口1：只负责极速读取本地CSV，绝对不阻塞
@app.route('/api/signals')
def get_signals():
    current_positions = []
    history_records = []
    update_time = "--"
    stats = None  # 新增：初始化统计数据对象

    if os.path.exists(CSV_FILE_PATH):
        try:
            mtime = os.path.getmtime(CSV_FILE_PATH)
            update_time = datetime.fromtimestamp(mtime).strftime("%Y-%m-%d %H:%M:%S")

            df = pd.read_csv(CSV_FILE_PATH)
            df.columns = df.columns.str.strip()

            target_cols = ['time', 'action', 'event', 'coin', 'price', 'target_weight', 'pnl']
            df = df[target_cols]
            df = df.rename(columns={'target_weight': 'weight'})

            # 转化为字典列表（保持CSV原始的正序：由旧到新，方便进行状态推演）
            raw_data = df.to_dict(orient='records')

            active_positions = {}

            for row in raw_data:
                # 清洗 NaN
                clean_row = {}
                for k, v in row.items():
                    if isinstance(v, float) and math.isnan(v):
                        clean_row[k] = None
                    else:
                        clean_row[k] = v

                coin = clean_row['coin']
                event = clean_row['event']

                if event == 'OPEN':
                    # 记录为当前持仓
                    active_positions[coin] = {
                        'coin': coin,
                        'action': clean_row['action'],
                        'open_time': clean_row['time'],
                        'open_price': clean_row['price'],
                        'weight': clean_row['weight']
                    }
                elif event == 'CLOSE':
                    # 发现平仓信号，与之前的开仓进行配对形成历史记录
                    if coin in active_positions:
                        open_pos = active_positions.pop(coin)  # 移出当前持仓
                        history_records.append({
                            'coin': coin,
                            'action': open_pos['action'],  # 取开仓方向
                            'open_time': open_pos['open_time'],
                            'close_time': clean_row['time'],
                            'open_price': open_pos['open_price'],
                            'close_price': clean_row['price'],
                            'pnl': clean_row['pnl']
                        })
                    else:
                        # 容错：如果日志一开始就是平仓（没记录到开仓）
                        history_records.append({
                            'coin': coin,
                            'action': clean_row['action'],
                            'open_time': '--',
                            'close_time': clean_row['time'],
                            'open_price': '--',
                            'close_price': clean_row['price'],
                            'pnl': clean_row['pnl']
                        })

            # 将字典转为列表并按时间倒序（最新的持仓在最前面）
            current_positions = list(active_positions.values())
            current_positions.sort(key=lambda x: x['open_time'], reverse=True)

            # --- 新增：计算仪表盘所需的统计数据 ---
            total_count = len(history_records)
            # 过滤出 pnl 不为 None 且大于 0 的盈利次数
            win_count = sum(1 for r in history_records if r.get('pnl') is not None and r['pnl'] > 0)
            # 累加所有有效平仓的 pnl
            total_pnl = sum(r['pnl'] for r in history_records if r.get('pnl') is not None)

            stats = {
                "total_pnl": round(total_pnl, 2),
                "win_rate": (win_count / total_count * 100) if total_count > 0 else 0.0,
                "win_count": win_count,
                "total_count": total_count,
                # 因为 raw_data 是按时间正序的，直接取第一条和最后一条的时间即可
                "start_time": raw_data[0]['time'] if raw_data else "--",
                "end_time": raw_data[-1]['time'] if raw_data else "--"
            }
            # --- 新增结束 ---

            # 历史记录也按平仓时间倒序（最新平仓的在最前）
            history_records.reverse()

        except Exception as e:
            print(f"数据处理报错: {e}")

    return jsonify({
        "update_time": update_time,
        "current_positions": current_positions,
        "history": history_records,
        "stats": stats  # 新增：将统计数据推给前端
    })


# 接口2：专门负责耗时的拉取更新操作 (使用 POST 方法更符合语义)
@app.route('/api/update', methods=['POST'])
def update_signals():
    try:
        fetch_new_df()  # 耗时操作
        return jsonify({"status": "success", "message": "数据更新完成"})
    except Exception as e:
        print(f"拉取新数据报错: {e}")
        return jsonify({"status": "error", "message": "更新失败"}), 500


if __name__ == '__main__':
    # 开启 threaded=True 支持并发请求（Flask默认行为，确保不被阻塞）
    app.run(debug=True, port=5001, threaded=True)