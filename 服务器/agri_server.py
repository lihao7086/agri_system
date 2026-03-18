# agri_server.py
# 智慧农业系统启动文件（同时启动MQTT服务+HTTP服务）
import threading
import time
from datetime import datetime
import uvicorn
from mqtt_handler import MQTTHandler
from http_api import app as http_app

# ===== 全局配置 =====
HTTP_HOST = "0.0.0.0"
HTTP_PORT = 8080

# ===== 启动MQTT服务（子线程） =====
def start_mqtt_service():
    """启动MQTT服务（独立线程）"""
    try:
        mqtt_handler = MQTTHandler()
        mqtt_handler.start()
        # 保持线程运行
        while True:
            time.sleep(1)
    except Exception as e:
        print(f"[{datetime.now()}] ❌ MQTT服务启动失败：{str(e)}")

# ===== 启动HTTP服务（主线程） =====
def start_http_service():
    """启动HTTP服务"""
    print(f"[{datetime.now()}] 🚀 HTTP服务即将启动：http://{HTTP_HOST}:{HTTP_PORT}")
    uvicorn.run(
        http_app,
        host=HTTP_HOST,
        port=HTTP_PORT,
        log_level="info"
    )

# ===== 主函数：统一启动 =====
if __name__ == "__main__":
    print(f"[{datetime.now()}] 🎯 智慧农业系统开始启动...")

    # 1. 启动MQTT服务（子线程）
    mqtt_thread = threading.Thread(target=start_mqtt_service, daemon=True)
    mqtt_thread.start()
    time.sleep(2)  # 等待MQTT服务启动完成

    # 2. 启动HTTP服务（主线程）
    try:
        start_http_service()
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] ⚠️ 智慧农业系统手动停止")
    except Exception as e:
        print(f"[{datetime.now()}] ❌ HTTP服务启动失败：{str(e)}")