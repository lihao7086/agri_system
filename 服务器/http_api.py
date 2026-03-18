# http_api.py
# 仅处理HTTP接口：最新数据、历史数据分页查询、最近7天平均值
import pymysql
from fastapi import FastAPI, Query
from datetime import datetime, date, timedelta
from typing import Optional

# 初始化FastAPI
app = FastAPI(
    title="智慧农业系统HTTP接口",
    description="提供传感器数据查询接口（最新数据/历史数据/最近7天平均值）",
    version="1.0.0"
)

# ===== 数据库配置 =====
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "mnbvcxz135",  # 替换为实际密码
    "database": "agri_system",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor
}

# ===== 通用数据库连接函数 =====
def get_db_connection():
    """获取数据库连接"""
    return pymysql.connect(**MYSQL_CONFIG)

# ===== HTTP接口：查询最新数据 =====
@app.get("/api/get_latest_data", summary="查询设备最新传感器数据")
def get_latest_data(
    device_id: str = Query(..., description="设备ID，如hi3861_001")
):
    """
    查询指定设备的最新一条传感器数据
    - device_id: 设备唯一标识（必填）
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 注意：原代码用report_time，需和你的数据库字段保持一致（如果是create_time请修改）
            sql = """
            SELECT device_id, temp, hum, co2, ph, soilMoist, light, create_time as report_time 
            FROM sensor_data 
            WHERE device_id=%s 
            ORDER BY create_time DESC 
            LIMIT 1
            """
            cursor.execute(sql, (device_id,))
            data = cursor.fetchone()
        conn.close()

        return {
            "code": 200,
            "msg": "查询成功",
            "data": data,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": f"查询失败：{str(e)}",
            "data": None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

# ===== HTTP接口：查询历史数据（分页+时间筛选） =====
@app.get("/api/get_history_data", summary="查询设备历史数据（分页）")
def get_history_data(
    device_id: str = Query(..., description="设备ID，如hi3861_001"),
    page: int = Query(1, ge=1, description="页码，默认1"),
    page_size: int = Query(20, ge=1, le=100, description="每页条数，1-100"),
    start_date: Optional[date] = Query(None, description="开始日期，格式YYYY-MM-DD"),
    end_date: Optional[date] = Query(None, description="结束日期，格式YYYY-MM-DD")
):
    """
    查询指定设备的历史传感器数据（支持分页和时间筛选）
    - device_id: 设备唯一标识（必填）
    - page: 页码，默认1
    - page_size: 每页条数，默认20（最大100）
    - start_date/end_date: 时间筛选，可选
    """
    try:
        conn = get_db_connection()
        with conn.cursor() as cursor:
            # 基础SQL（注意：report_time改为create_time，和你的数据库字段一致）
            sql_base = """
            SELECT device_id, temp, hum, co2, ph, soilMoist, light, create_time as report_time 
            FROM sensor_data 
            WHERE device_id=%s
            """
            params = [device_id]

            # 时间筛选条件
            if start_date:
                sql_base += " AND DATE(create_time) >= %s"
                params.append(start_date)
            if end_date:
                sql_base += " AND DATE(create_time) <= %s"
                params.append(end_date)

            # 总条数查询
            sql_count = f"SELECT COUNT(*) as total FROM ({sql_base}) as t"
            cursor.execute(sql_count, params)
            total = cursor.fetchone()["total"]

            # 分页数据查询
            offset = (page - 1) * page_size
            sql_data = sql_base + " ORDER BY create_time DESC LIMIT %s OFFSET %s"
            params.extend([page_size, offset])
            cursor.execute(sql_data, params)
            data_list = cursor.fetchall()

        conn.close()

        # 计算总页数
        total_pages = (total + page_size - 1) // page_size if total > 0 else 0

        return {
            "code": 200,
            "msg": "查询成功",
            "data": {
                "list": data_list,       # 当前页数据列表
                "pagination": {
                    "page": page,        # 当前页码
                    "page_size": page_size,  # 每页条数
                    "total": total,      # 总条数
                    "total_pages": total_pages  # 总页数
                }
            },
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": f"查询失败：{str(e)}",
            "data": None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

# ===== 新增：查询最近7天平均值数据 =====
@app.get("/api/get_7days_avg", summary="查询设备最近7天传感器平均值")
def get_7days_avg(
    device_id: str = Query(..., description="设备ID，如hi3861_001")
):
    """
    查询指定设备最近7天的每日传感器平均值（自动计算时间范围，无需传日期）
    - device_id: 设备唯一标识（必填）
    - 返回数据按日期升序排列（从7天前到今天）
    """
    try:
        # 1. 计算时间范围：最近7天（今天 - 6天 至 今天）
        end_date = date.today()
        start_date = end_date - timedelta(days=6)

        # 2. 连接数据库查询平均值表
        conn = get_db_connection()
        with conn.cursor() as cursor:
            sql = """
            SELECT 
                date, 
                temp_avg, 
                hum_avg, 
                co2_avg, 
                ph_avg, 
                soilMoist_avg, 
                light_avg 
            FROM sensor_daily_avg 
            WHERE device_id = %s 
            AND date BETWEEN %s AND %s 
            ORDER BY date ASC
            """
            cursor.execute(sql, (device_id, start_date, end_date))
            avg_list = cursor.fetchall()

        conn.close()

        # 3. 构造返回结果（容错：数据不足7天也返回成功）
        return {
            "code": 200,
            "msg": "查询成功",
            "data": {
                "device_id": device_id,
                "time_range": f"{start_date} 至 {end_date}",
                "days_count": len(avg_list),  # 实际有数据的天数
                "avg_list": avg_list          # 平均值列表（空列表则无数据）
            },
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": f"查询失败：{str(e)}",
            "data": None,
            "timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        }

# ===== 健康检查接口 =====
@app.get("/api/health", summary="服务健康检查")
def health_check():
    """检查HTTP服务和数据库连接是否正常"""
    try:
        conn = get_db_connection()
        conn.ping()
        conn.close()
        return {
            "code": 200,
            "msg": "服务正常",
            "data": {
                "http_status": "running",
                "db_status": "connected",
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }
    except Exception as e:
        return {
            "code": 500,
            "msg": "服务异常",
            "data": {
                "http_status": "running",
                "db_status": f"disconnected: {str(e)}",
                "time": datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            }
        }

# ===== 测试启动 =====
if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8080)