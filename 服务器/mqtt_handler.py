# mqtt_handler.py
# 仅处理MQTT：硬件数据上报、设备控制指令（灯/水阀/风扇）、告警推送、设备状态转发到App
import paho.mqtt.client as mqtt
import pymysql
import json
import time
from datetime import datetime
import threading

# ===== 自定义序列化函数 =====
def json_serial(obj):
    if isinstance(obj, datetime):
        return obj.strftime("%Y-%m-%d %H:%M:%S")
    raise TypeError(f"Type {type(obj)} not serializable")

# ===== 核心配置（按要求修正设备类型） =====
# MQTT配置
MQTT_BROKER = "115.175.12.81"
MQTT_PORT = 1883
MQTT_USER = "admin"
MQTT_PASS = "public"

# MySQL配置
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PWD = "mnbvcxz135"  # 替换为实际MySQL密码
MYSQL_DB = "agri_system"

# 设备类型定义（最终版）
# light: 灯（不变）
# valve: 水阀
# pump: 风扇
DEVICE_TYPES = ["light", "valve", "pump"]

# 指令动作定义
ACTION_ON = "on"
ACTION_OFF = "off"

# App端状态推送主题（所有App订阅该主题接收设备状态）
APP_STATUS_TOPIC = "agri/app/device_status"

# ===== 数据库操作：保存传感器上报数据 =====
def save_sensor_data(device_id: str, data: dict) -> dict:
    """硬件数据写入数据库（过滤核心字段全0的无效数据）"""
    try:
        # 解析数据（兼容告警/正常格式）
        if data.get("type") == "alert":
            temp = 0.0
            hum = 0
            co2 = 0.0
            ph = 0.0
            soilMoist = 0
            light = float(data.get("value", 0.0))
            report_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        else:
            data_content = data.get("data", data)
            temp = float(data_content.get("temp", 0.0))
            hum = int(data_content.get("hum", 0))
            co2 = float(data_content.get("co2", 0.0))
            ph = float(data_content.get("ph", 0.0))
            soilMoist = int(data_content.get("soilMoist", 0))
            light = int(data_content.get("light", 0))
            report_time = data_content.get("report_time", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))

        # 核心字段全0则拒绝写入
        is_invalid = (temp == 0.0 and hum == 0 and co2 == 0.0 and 
                      ph == 0.0 and soilMoist == 0)
        if is_invalid:
            return {"code": 400, "msg": "Rejected: core sensor values are all zero"}

        # 数据库连接
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PWD,
            database=MYSQL_DB,
            charset='utf8mb4',
            use_unicode=True,
            cursorclass=pymysql.cursors.DictCursor
        )
        with conn.cursor() as cursor:
            sql = """
            INSERT INTO sensor_data (device_id, temp, hum, co2, ph, soilMoist, light, report_time)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(sql, (device_id, temp, hum, co2, ph, soilMoist, light, report_time))
        conn.commit()
        conn.close()

        # 告警判断：温度>35℃推送告警
        alert_msg = {
            "alert_type": "temp_high",
            "device_id": device_id,
            "value": temp,
            "threshold": 35.0,
            "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
        # 发布告警到MQTT Topic（App可订阅）
        client = mqtt.Client()  # 临时客户端用于发布告警（避免self依赖）
        client.username_pw_set(MQTT_USER, MQTT_PASS)
        client.connect(MQTT_BROKER, MQTT_PORT, 60)
        client.publish(
            f"agri/device/{device_id}/alert", 
            json.dumps(alert_msg), 
            qos=1,
            retain=False
        )
        client.disconnect()

        return {"code": 200, "msg": "Data saved successfully"}
    except Exception as e:
        return {"code": 500, "msg": f"Save failed: {str(e)}"}

# ===== MQTT客户端核心类 =====
class MQTTHandler:
    def __init__(self):
        self.client = mqtt.Client()
        self.client.username_pw_set(MQTT_USER, MQTT_PASS)
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.client.on_disconnect = self.on_disconnect
        
        # 类属性
        self.device_last_active = {}  # 记录设备最后活跃时间
        self.device_online_status = {}  # 设备在线状态缓存
        
        self.check_interval = 30  # 检测间隔30秒
        self.offline_threshold = 30  # 超过30秒判定离线
        self._start_heartbeat_checker()

    def on_connect(self, client, userdata, flags, rc):
        if rc == 0:
            print(f"[{datetime.now()}] ✅ MQTT服务启动成功")
            # 订阅Topic：硬件上报 + 控制指令 + 设备状态 + 设备开关状态
            client.subscribe([
                ("agri/device/+/data/upload", 1),    # 传感器数据上报
                ("agri/device/+/control", 1),        # 设备控制指令（灯/水阀/风扇）
                ("agri/device/+/status", 1),         # 设备在线状态上报
                ("agri/device/+/device_status", 1)   # 新增：设备开关状态上报
            ])
            print(f"[{datetime.now()}] 📢 已订阅所有MQTT Topic（含控制指令+设备状态+设备开关状态）")
        else:
            print(f"[{datetime.now()}] ❌ MQTT连接失败，错误码：{rc}")

    def on_disconnect(self, client, userdata, rc):
        if rc != 0:
            print(f"[{datetime.now()}] ⚠️ MQTT断开连接，5秒后自动重连...")
            time.sleep(5)
            client.reconnect()

    def _forward_status_to_app(self, device_id: str, status_msg: dict):
        """将设备状态转发到App端的MQTT主题"""
        try:
            # 构造标准化的App端状态消息
            app_status_msg = {
                "type": "device_online_status",
                "device_id": device_id,
                "status": "online" if status_msg.get("online") else "offline",
                "hardware_report_time": status_msg.get("time", datetime.now().strftime("%Y-%m-%d %H:%M:%S")),
                "server_forward_time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "trigger": "heartbeat" if not status_msg.get("online") else "data_upload"  # 标记为数据上报触发
            }
            # 发布到App端主题（QoS=1确保送达，retain=True保留最后一条状态）
            self.client.publish(
                APP_STATUS_TOPIC,
                json.dumps(app_status_msg, ensure_ascii=False),
                qos=1,
                retain=True
            )
            print(f"[{datetime.now()}] 📤 已转发[{device_id}]在线状态到App：{json.dumps(app_status_msg)}")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ 转发状态到App失败：{str(e)}")

    def _save_device_switch_status(self, device_id: str, device_type: str, status: str):
        """保存设备开关状态到数据库"""
        try:
            conn = pymysql.connect(
                host=MYSQL_HOST,
                user=MYSQL_USER,
                password=MYSQL_PWD,
                database=MYSQL_DB,
                charset='utf8mb4',
                use_unicode=True,
                cursorclass=pymysql.cursors.DictCursor
            )
            with conn.cursor() as cursor:
                # 先更新现有记录（如果存在），不存在则插入
                sql = """
                INSERT INTO device_switch_status (device_id, device_type, status, update_time)
                VALUES (%s, %s, %s, %s)
                ON DUPLICATE KEY UPDATE
                status = %s, update_time = %s
                """
                update_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                cursor.execute(sql, (device_id, device_type, status, update_time, status, update_time))
            conn.commit()
            conn.close()
            print(f"[{datetime.now()}] 📥 [{device_id}] {device_type} 状态({status})已存入数据库")
        except Exception as e:
            print(f"[{datetime.now()}] ❌ 保存设备开关状态失败：{str(e)}")

    def on_message(self, client, userdata, msg):
        try:
            topic = msg.topic
            payload = json.loads(msg.payload.decode("utf-8"))
            timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            # 1. 处理传感器数据上报（核心修改：每次收到都推在线）
            if topic.endswith("/data/upload"):
                device_id = topic.split("/")[2]
                print(f"[{timestamp}] 📥 收到[{device_id}]传感器数据：{json.dumps(payload)}")
                
                # 更新设备最后活跃时间
                self.device_last_active[device_id] = datetime.now()
                # 更新在线状态为在线
                self.device_online_status[device_id] = True
                
                # ========== 核心修改：每次收到数据都主动推送在线消息 ==========
                print(f"[{timestamp}] 🎯 收到[{device_id}]数据，推送在线状态到App")
                self._forward_status_to_app(device_id, {"online": True, "time": timestamp})
                
                result = save_sensor_data(device_id, payload)
                # 发布上报结果反馈
                ack_topic = f"agri/device/{device_id}/data/ack"
                ack_payload = json.dumps(result, ensure_ascii=False, default=json_serial).encode('utf-8')
                client.publish(ack_topic, ack_payload, qos=1)
                print(f"[{timestamp}] 📤 [{device_id}] 数据保存结果：{result['msg']}")

            # 2. 处理设备控制指令（核心：灯/水阀/风扇）
            elif topic.endswith("/control"):
                device_id = topic.split("/")[2]
                print(f"[{timestamp}] 📥 收到[{device_id}]控制指令：{json.dumps(payload)}")

                # 解析指令参数
                device_type = payload.get("device_type")
                action = payload.get("action")

                # 校验参数合法性
                if not device_type or not action:
                    error_msg = "参数错误：device_type 和 action 不能为空"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    client.publish(
                        f"agri/device/{device_id}/control/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return

                # 校验设备类型（仅支持light/valve/pump）
                if device_type not in DEVICE_TYPES:
                    error_msg = f"不支持的设备类型：{device_type}，仅支持：{DEVICE_TYPES}"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    client.publish(
                        f"agri/device/{device_id}/control/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return

                # 校验指令动作（仅支持on/off）
                if action not in [ACTION_ON, ACTION_OFF]:
                    error_msg = f"不支持的指令：{action}，仅支持：{ACTION_ON}/{ACTION_OFF}"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    client.publish(
                        f"agri/device/{device_id}/control/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return

                # ✅ 指令验证通过，执行控制逻辑
                control_result = {
                    "code": 200,
                    "msg": f"指令执行成功：{device_type} -> {action}",
                    "device_id": device_id,
                    "device_type": device_type,
                    "action": action,
                    "execute_time": timestamp
                }
                print(f"[{timestamp}] ✅ {control_result['msg']}")

                # 发布控制结果反馈
                client.publish(
                    f"agri/device/{device_id}/control/ack",
                    json.dumps(control_result, ensure_ascii=False, default=json_serial),
                    qos=1
                )

            # 3. 处理设备在线状态上报 + 转发到App
            elif topic.endswith("/status"):
                device_id = topic.split("/")[2]
                # 更新本地缓存（类属性）
                self.device_online_status[device_id] = payload.get("online", False)
                
                # 更新设备最后活跃时间
                if payload.get("online"):
                    self.device_last_active[device_id] = datetime.now()
                
                print(f"[{timestamp}] 🟢 [{device_id}] 设备状态：{'在线' if payload.get('online') else '离线'}")
                
                # 核心：转发状态到App端MQTT主题
                self._forward_status_to_app(device_id, payload)

            # 4. 新增：处理设备（valve/light/pump）开关状态上报 + 转发到App
            elif topic.endswith("/device_status"):
                device_id = topic.split("/")[2]
                print(f"[{timestamp}] 📥 收到[{device_id}]设备开关状态：{json.dumps(payload)}")

                # 校验必填参数
                device_type = payload.get("device_type")
                status = payload.get("status")  # 取值：on/off
                if not device_type or not status:
                    error_msg = "参数错误：device_type（light/valve/pump）和 status（on/off）不能为空"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    # 发布错误反馈给硬件
                    client.publish(
                        f"agri/device/{device_id}/device_status/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return

                # 校验设备类型和状态合法性
                if device_type not in DEVICE_TYPES:
                    error_msg = f"不支持的设备类型：{device_type}，仅支持：{DEVICE_TYPES}"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    client.publish(
                        f"agri/device/{device_id}/device_status/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return
                if status not in [ACTION_ON, ACTION_OFF]:
                    error_msg = f"不支持的状态：{status}，仅支持：{ACTION_ON}/{ACTION_OFF}"
                    print(f"[{timestamp}] ❌ {error_msg}")
                    client.publish(
                        f"agri/device/{device_id}/device_status/ack",
                        json.dumps({"code":400, "msg": error_msg}, ensure_ascii=False),
                        qos=1
                    )
                    return

                # ✅ 参数校验通过：构造App端的标准化状态消息
                app_device_status_msg = {
                    "type": "device_switch_status",  # 区分在线状态/开关状态
                    "device_id": device_id,
                    "device_type": device_type,  # light/valve/pump
                    "status": status,  # on/off
                    "hardware_report_time": payload.get("report_time", timestamp),
                    "server_forward_time": timestamp
                }

                # 转发到App端统一主题（QoS=1确保送达，retain=True保留最新状态）
                self.client.publish(
                    APP_STATUS_TOPIC,
                    json.dumps(app_device_status_msg, ensure_ascii=False),
                    qos=1,
                    retain=True
                )
                print(f"[{timestamp}] 📤 已转发[{device_id}] {device_type} 状态到App：{json.dumps(app_device_status_msg)}")

                # （可选）将设备开关状态存入数据库
                self._save_device_switch_status(device_id, device_type, status)

                # 发布成功反馈给硬件
                success_msg = {
                    "code": 200,
                    "msg": f"{device_type}状态上报成功：{status}",
                    "device_id": device_id,
                    "device_type": device_type,
                    "status": status,
                    "server_time": timestamp
                }
                client.publish(
                    f"agri/device/{device_id}/device_status/ack",
                    json.dumps(success_msg, ensure_ascii=False),
                    qos=1
                )

        except Exception as e:
            print(f"[{datetime.now()}] ❌ MQTT消息处理失败：{str(e)}")

    def start(self):
        """启动MQTT服务（后台运行）"""
        self.client.connect(MQTT_BROKER, MQTT_PORT, 60)
        self.client.loop_start()
        print(f"[{datetime.now()}] 🚀 MQTT服务已启动（支持灯/水阀/风扇控制+设备状态转发）")

    def _start_heartbeat_checker(self):
        """启动心跳检测线程"""
        def check_devices():
            while True:
                time.sleep(self.check_interval)
                current_time = datetime.now()
                for device_id, last_active in self.device_last_active.items():
                    # 计算时间差（秒）
                    time_diff = (current_time - last_active).total_seconds()
                    
                    # 检测离线
                    if time_diff > self.offline_threshold and self.device_online_status.get(device_id, False):
                        print(f"[{current_time}] ⚠️ 检测到设备[{device_id}]离线（30秒无数据）")
                        self._forward_status_to_app(device_id, {"online": False, "time": current_time.strftime("%Y-%m-%d %H:%M:%S")})
                        # 更新在线状态为离线
                        self.device_online_status[device_id] = False
        
        # 启动守护线程
        checker_thread = threading.Thread(target=check_devices, daemon=True)
        checker_thread.start()

# ===== 测试启动入口 =====
if __name__ == "__main__":
    mqtt_handler = MQTTHandler()
    mqtt_handler.start()
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"[{datetime.now()}] ⚠️ MQTT服务手动停止")