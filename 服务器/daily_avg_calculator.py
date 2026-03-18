# agri/daily_avg_calculator.py (Final Stable Version)
import pymysql
import os
from datetime import datetime, date, timedelta

# ===== Path Configuration =====
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
LOG_FILE = os.path.join(BASE_DIR, "logs", "daily_avg.log")
os.makedirs(os.path.join(BASE_DIR, "logs"), exist_ok=True)

# ===== MySQL Configuration =====
MYSQL_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "mnbvcxz135".encode('utf-8'),  # Replace with your password
    "database": "agri_system",
    "charset": "utf8mb4",
    "cursorclass": pymysql.cursors.DictCursor,
    "init_command": "SET NAMES utf8mb4"
}

# ===== Log Function (100% Clean, No Special Chars) =====
def log(msg):
    log_msg = f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] {msg}\n"
    print(log_msg.strip())
    # Write log with error ignore (compatible with latin-1)
    with open(LOG_FILE, "a", encoding="latin-1", errors="ignore") as f:
        f.write(log_msg)

# ===== Core Calculation Function =====
def calculate_daily_avg(target_date: date = None):
    if not target_date:
        target_date = date.today() - timedelta(days=1)
    target_date_str = target_date.strftime("%Y-%m-%d")
    device_id = "hi3861_001"

    try:
        # Connect to MySQL
        conn = pymysql.connect(**MYSQL_CONFIG)
        cursor = conn.cursor()
        log(f"Start calculating daily average for {target_date_str} (device: {device_id})")

        # Query valid data (use create_time)
        query_sql = """
        SELECT temp, hum, co2, ph, soilMoist, light 
        FROM sensor_data 
        WHERE device_id=%s 
        AND DATE(create_time) = %s
        AND (temp != 0 OR hum != 0 OR co2 != 0 OR ph != 0 OR soilMoist != 0 OR light != 0)
        """
        cursor.execute(query_sql, (device_id, target_date_str))
        data_list = cursor.fetchall()

        if not data_list:
            log(f"No valid data for {target_date_str}, skip calculation")
            conn.close()
            return

        # Calculate average (safe float conversion)
        total = len(data_list)
        temp_sum = hum_sum = co2_sum = ph_sum = soilMoist_sum = light_sum = 0.0
        for data in data_list:
            temp_sum += float(data["temp"] or 0)
            hum_sum += float(data["hum"] or 0)
            co2_sum += float(data["co2"] or 0)
            ph_sum += float(data["ph"] or 0)
            soilMoist_sum += float(data["soilMoist"] or 0)
            light_sum += float(data["light"] or 0)

        # Calculate average (round to 2 decimals)
        avg_data = {
            "device_id": device_id,
            "date": target_date_str,
            "temp_avg": round(temp_sum / total, 2),
            "hum_avg": round(hum_sum / total, 2),
            "co2_avg": round(co2_sum / total, 2),
            "ph_avg": round(ph_sum / total, 2),
            "soilMoist_avg": round(soilMoist_sum / total, 2),
            "light_avg": round(light_sum / total, 2)
        }

        # Insert/Update average data (safe values)
        upsert_sql = """
        INSERT INTO sensor_daily_avg 
        (device_id, date, temp_avg, hum_avg, co2_avg, ph_avg, soilMoist_avg, light_avg)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
        ON DUPLICATE KEY UPDATE
        temp_avg=VALUES(temp_avg),
        hum_avg=VALUES(hum_avg),
        co2_avg=VALUES(co2_avg),
        ph_avg=VALUES(ph_avg),
        soilMoist_avg=VALUES(soilMoist_avg),
        light_avg=VALUES(light_avg),
        create_time=CURRENT_TIMESTAMP
        """
        cursor.execute(upsert_sql, (
            avg_data["device_id"], avg_data["date"],
            avg_data["temp_avg"], avg_data["hum_avg"],
            avg_data["co2_avg"], avg_data["ph_avg"],
            avg_data["soilMoist_avg"], avg_data["light_avg"]
        ))
        conn.commit()

        # Log success (no special chars)
        log(f"Daily average calculated successfully for {target_date_str}:")
        log(f"Temperature: {avg_data['temp_avg']}, Humidity: {avg_data['hum_avg']}, CO2: {avg_data['co2_avg']}")
        log(f"PH: {avg_data['ph_avg']}, Soil Moisture: {avg_data['soilMoist_avg']}, Light: {avg_data['light_avg']}")

    except Exception as e:
        # Log error (no special chars)
        log(f"Calculation failed: {str(e)}")
        if 'conn' in locals() and conn.open:
            conn.rollback()
    finally:
        if 'conn' in locals() and conn.open:
            conn.close()

# ===== Run Calculation =====
if __name__ == "__main__":
    calculate_daily_avg()