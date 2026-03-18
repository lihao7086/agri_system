import pymysql
from config import *

# 获取数据库连接
def get_mysql_conn():
    try:
        conn = pymysql.connect(
            host=MYSQL_HOST,
            user=MYSQL_USER,
            password=MYSQL_PWD,
            database=MYSQL_DB,
            charset='utf8mb4'
        )
        return conn
    except Exception as e:
        print("MySQL连接失败:", e)
        return None

# ===================== 1. 存储环境传感器数据 =====================
def save_sensor_data(device_id, temp, humidity, co2, ph, light):
    conn = get_mysql_conn()
    if not conn:
        return False

    cursor = conn.cursor()
    try:
        sql = """
        INSERT INTO sensor_data
        (device_id, temp, humidity, co2, ph, light)
        VALUES (%s, %s, %s, %s, %s, %s)
        """
        cursor.execute(sql, (
            device_id, temp, humidity, co2, ph, light
        ))
        conn.commit()
        print(f"✅ 环境数据已自动入库：{device_id}")
        return True
    except Exception as e:
        print("❌ 环境数据存储失败:", e)
        return False
    finally:
        cursor.close()
        conn.close()

# ===================== 2. 记录设备启动（灌溉/灯光/通风） =====================
def add_device_run_log(device_id, device_type, start_time, end_time):
    conn = get_mysql_conn()
    if not conn:
        return False

    cursor = conn.cursor()
    try:
        sql = """
        INSERT INTO device_run_log
        (device_id, device_type, start_time, end_time, status)
        VALUES (%s, %s, %s, %s, 'running')
        """
        cursor.execute(sql, (
            device_id, device_type, start_time, end_time
        ))
        conn.commit()
        print(f"✅ 设备记录已自动入库：{device_type}")
        return True
    except Exception as e:
        print("❌ 设备记录存储失败:", e)
        return False
    finally:
        cursor.close()
        conn.close()

# ===================== 3. 查询正在运行的设备 =====================
def get_running_devices(device_id):
    conn = get_mysql_conn()
    if not conn:
        return []

    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        sql = """
        SELECT * FROM device_run_log
        WHERE device_id=%s AND status='running'
        """
        cursor.execute(sql, (device_id,))
        return cursor.fetchall()
    except Exception as e:
        print("查询失败:", e)
        return []
    finally:
        cursor.close()
        conn.close()

# 新增：更新指令执行结果（供MQTT回调使用，解决导入报错）
def update_cmd_result(device_id, cmd, result):
    conn = get_mysql_conn()
    if not conn:
        print("❌ 数据库连接失败，无法更新指令结果")
        return False
    
    cursor = conn.cursor()
    try:
        # 更新控制指令的执行结果和状态
        cursor.execute(
            """UPDATE control_cmd 
               SET exec_result=%s, pull_status=1 
               WHERE device_id=%s AND cmd=%s AND pull_status=0 
               ORDER BY create_time DESC LIMIT 1""",
            (result, device_id, cmd)
        )
        conn.commit()
        print(f"✅ 设备{device_id}指令[{cmd}]结果更新：{result}")
        return True
    except Exception as e:
        print(f"❌ 更新指令结果失败：{e}")
        conn.rollback()
        return False
    finally:
        cursor.close()
        conn.close()

# 新增：CSV批量导入传感器数据（解决import_csv_data导入报错）
def import_csv_data(csv_path, device_id):
    """从CSV文件批量导入历史传感器数据"""
    conn = get_mysql_conn()
    if not conn:
        return {"code":500, "msg":"数据库连接失败"}
    
    cursor = conn.cursor()
    try:
        # 读取CSV文件（需确保CSV列：temp,humidity,co2,ph,light,create_time）
        import csv
        with open(csv_path, "r", encoding="utf8") as f:
            reader = csv.DictReader(f)
            count = 0
            for row in reader:
                cursor.execute(
                    """INSERT INTO sensor_data 
                       (device_id, temp, humidity, co2, ph, light, create_time) 
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (device_id, row["temp"], row["humidity"], row["co2"], row["ph"], row["light"], row["create_time"])
                )
                count += 1
        conn.commit()
        return {"code":200, "msg":f"成功导入{count}条数据"}
    except Exception as e:
        conn.rollback()
        return {"code":500, "msg":f"导入失败：{str(e)}"}
    finally:
        cursor.close()
        conn.close()

# 新增：查询设备最新传感器数据（解决get_latest_sensor_data导入报错）
def get_latest_sensor_data(device_id):
    """查询指定设备的最新环境传感器数据"""
    conn = get_mysql_conn()
    if not conn:
        return None
    
    cursor = conn.cursor(pymysql.cursors.DictCursor)
    try:
        cursor.execute(
            """SELECT * FROM sensor_data 
               WHERE device_id=%s 
               ORDER BY create_time DESC LIMIT 1""",
            (device_id,)
        )
        return cursor.fetchone()  # 返回最新一条数据
    except Exception as e:
        print(f"❌ 查询最新数据失败：{e}")
        return None
    finally:
        cursor.close()
        conn.close()