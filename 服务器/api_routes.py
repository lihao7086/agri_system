# 接口路由（所有FastAPI接口定义）# api_routes.py
from fastapi import Header, HTTPException
from config import API_TOKEN
from db_operations import get_mysql_conn, import_csv_data, get_latest_sensor_data

# 鉴权函数
def check_token(token: str = Header(None)):
    if token != API_TOKEN:
        raise HTTPException(status_code=401, detail="无效的API Token，拒绝访问")

# 注册接口路由的函数（供主文件调用）
def register_routes(app):
    # 1. 注册设备接口
    @app.post("/register_device")
    async def register_device(device_id: str, device_name: str, token: str = Header(None)):
        check_token(token)
        conn = get_mysql_conn()
        if not conn:
            return {"code": 500, "msg": "数据库连接失败", "data": None}
        
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT * FROM device WHERE device_id=%s", (device_id,))
            if cursor.fetchone():
                return {"code": 400, "msg": "设备已注册", "data": None}
            
            cursor.execute(
                "INSERT INTO device (device_id, device_name, status) VALUES (%s, %s, 0)",
                (device_id, device_name)
            )
            conn.commit()
            return {"code": 200, "msg": "设备注册成功", "data": {"deviceId": device_id, "deviceName": device_name}}
        except Exception as e:
            conn.rollback()
            return {"code": 500, "msg": f"注册失败：{str(e)}", "data": None}
        finally:
            cursor.close()
            conn.close()

    # 2. CSV导入接口
    @app.post("/import_csv")
    async def import_csv(csv_path: str, device_id: str, token: str = Header(None)):
        check_token(token)
        result = import_csv_data(csv_path, device_id)
        return result

    # 3. 查询最新数据接口
    @app.get("/get_latest_data")
    async def get_latest_data(device_id: str, token: str = Header(None)):
        check_token(token)
        data = get_latest_sensor_data(device_id)
        if not data:
            return {"code": 404, "msg": "暂无传感器数据", "data": None}
        return {"code": 200, "msg": "查询成功", "data": data}

    # 其他接口（如send_cmd、get_device_status等）按此格式添加
    return app