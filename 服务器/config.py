# config.py 配置文件（MySQL/MQTT/Token等参数）

# MySQL配置
MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PWD = "mnbvcxz135"  
MYSQL_DB = "agri_system"

# MQTT配置
MQTT_BROKER = "115.175.12.81"
MQTT_PORT = 1883
MQTT_USER = "admin"
MQTT_PASS = "public"

# 接口配置
API_TOKEN = "mnbvcxz135"
BASE_URL = "http://115.175.12.81:8080"

# Topic定义
DATA_TOPIC = "agri/device/{}/data"
CMD_TOPIC = "agri/device/{}/cmd"
RESULT_TOPIC = "agri/device/{}/result"