import {
  MqttAsync,
  MqttClient,
  MqttClientOptions,
  MqttConnectOptions,
  MqttSubscribeOptions,
  MqttPublishOptions,
  MqttMessage,
  MqttResponse
} from '@ohos/mqtt';
import hilog from '@ohos.hilog';
import { BusinessError } from '@ohos.base';
import preferences from '@ohos.data.preferences';
import common from '@ohos.app.ability.common';

// ===================== 0. 类型声明（适配 ArkTS 严格类型） =====================
export interface ServerConfig {
  mqttIP: string;
  mqttPort: number;
  mqttUsername: string;
  mqttPassword: string;
  deviceId: string;
}

export interface MqttTopics {
  /** APP 订阅此主题，接收服务器返回的查询数据（表序号4 result） */
  sensorData: string;
  /** APP 向此主题发布请求，查询最新传感器数据（表序号3 query） */
  queryRequest: string;
  controlCmd: string;
  execResult: string;
  deviceStatus: string;
  /** 硬件上报：具体设备真实开关状态（on/off） */
  deviceSwitchStatus: string;
  /** 服务器 → APP：周报分析与预测（Markdown 文本） */
  weeklyReport: string;
  /** APP → 服务器：AI 问答提问 */
  aiQuestion: string;
  /** 服务器 → APP：AI 问答回答 */
  aiAnswer: string;
}

export interface SensorDataModel {
  temperature: number;
  humidity: number;
  light: number;
  co2: number;
  ph: number;
  soilMoist: number;
  reportTime: string;
}

export interface ControlStatusModel {
  lightBright: number;
  pumpSpeed: number;
  valveState: boolean;
}

export interface ExecResultModel {
  code: number;
  message: string;
  command: string;
}

export type ThresholdMetric = 'temp' | 'airHum' | 'light' | 'co2' | 'soilHum' | 'ph';

export interface ThresholdConfigItem {
  metric: ThresholdMetric;
  minValue: number;
  maxValue: number;
  hysteresis: number;
}

export interface ThresholdConfigModel {
  items: ThresholdConfigItem[];
}

export type SensorDataListener = (data: SensorDataModel) => void;
export type DeviceStatusListener = (online: boolean, time?: string) => void;
export type DeviceControlStateListener = (state: ControlStatusModel) => void;

// result 主题的 JSON：成功 { code:200, data:{...} }，失败 { code:500, msg:"..." }
interface SensorResponsePayload {
  code?: number;
  msg?: string;
  data?: SensorDataPayload;
}

interface SensorDataPayload {
  device_id?: string;
  temp?: number;
  hum?: number;
  co2?: number;
  ph?: number;
  soilMoist?: number;
  light?: number;
  report_time?: string;
}

interface ExecResultPayload {
  code?: number;
  status?: string;
  command?: string;
}

/** 服务器在收到硬件上下线后向 agri/app/device_status 推送的格式（兼容两种） */
interface DeviceStatusPayload {
  online?: boolean;
  time?: string; // 如 "2026-03-14 17:00:00"
  type?: string; // 如 "device_online_status"
  device_id?: string;
  // 兜底兼容：服务端误发 device_switch_status 到主机主题时可能带此字段
  device_type?: string;
  status?: string; // 如 "online" | "offline"
  hardware_report_time?: string;
  server_forward_time?: string;
  trigger?: string;
}

/** 硬件上报具体设备开关状态：agri/device/{deviceId}/device_status */
interface DeviceSwitchStatusPayload {
  type?: string; // "device_switch_status"
  device_id?: string;
  device_type?: DeviceType; // "light" | "valve" | "pump"
  status?: DeviceAction; // "on" | "off"
  hardware_report_time?: string;
  server_forward_time?: string;
}

// 统一的MQTT业务错误类型（解决类型断言报错）
// 正确写法
interface MqttBusinessError {
  message?: string;
  code?: number;
  stack?: string;
}

// 控制指令结构（与汤俊琪解析逻辑一致）
interface ControlCommandPayload {
  light: number;
  pump: number;
  valve: number;
  device_id: string;
  timestamp: number;
}

export type DeviceType = 'valve' | 'light' | 'pump';
export type DeviceAction = 'on' | 'off';

/** 硬件在 device_status 主题上上报的开关状态（供页面判断开启/关闭是否被硬件确认） */
export interface DeviceSwitchReportModel {
  device_type: DeviceType;
  status: DeviceAction;
  hardware_report_time?: string;
}

export type DeviceSwitchReportListener = (report: DeviceSwitchReportModel) => void;

/** 服务器推送的周报 JSON（agri/server/weekly_report） */
export interface WeeklyReportModel {
  code: number;
  device_id: string;
  report: string;
  timestamp: string;
  data_period: string;
}

export type WeeklyReportListener = (data: WeeklyReportModel) => void;

export interface AiAnswerModel {
  code: number;
  device_id: string;
  question: string;
  answer: string;
  timestamp: string;
}

export type AiAnswerListener = (data: AiAnswerModel) => void;

interface DeviceActionPayload {
  device_type: DeviceType;
  action: DeviceAction;
}

interface WeeklyReportPayload {
  code?: number;
  device_id?: string;
  report?: string;
  timestamp?: string;
  data_period?: string;
}

interface AiQuestionPayload {
  device_id: string;
  question: string;
  timestamp: string;
}

interface AiAnswerPayload {
  device_id?: string;
  question?: string;
  answer?: string;
  timestamp?: string;
  code?: number;
}

export const SERVER_CONFIG: ServerConfig = {
  mqttIP: '115.175.12.81',       
  mqttPort: 1883,               
  mqttUsername: 'admin',        
  mqttPassword: 'public',        
  deviceId: 'hi3861_001'        
};


export const MQTT_TOPICS: MqttTopics = {
  // 序号4：服务器→APP，服务器返回查询到的数据（APP 订阅 result 收数据）
  sensorData: `agri/device/${SERVER_CONFIG.deviceId}/result`,
  // 序号3：APP→服务器，查询最新传感器数据（APP 往 query 发请求）
  queryRequest: `agri/device/${SERVER_CONFIG.deviceId}/query`,
  // 序号5：下发控制指令（水泵/灯光等）
  controlCmd: `agri/device/${SERVER_CONFIG.deviceId}/control`,
  // 序号4：执行结果也通过 result 返回
  execResult: `agri/device/${SERVER_CONFIG.deviceId}/result`,
  // 服务器收到硬件上下线后向 App 推送设备状态（App 订阅此主题）
  deviceStatus: 'agri/app/device_status',
  // 硬件上报：具体设备开关真实状态
  deviceSwitchStatus: `agri/device/${SERVER_CONFIG.deviceId}/device_status`,
  // 服务器 → APP：周报分析
  weeklyReport: 'agri/server/weekly_report',
  // APP → 服务器：AI 问答问题
  aiQuestion: 'agri/app/question',
  // 服务器 → APP：AI 问答回答
  aiAnswer: 'agri/server/answer'
};

export const sensorData: SensorDataModel = {
  temperature: 1,
  humidity: 0,
  light: 0,
  co2: 0,
  ph: 0,
  soilMoist: 0,
  reportTime: ''
};

export const controlStatus: ControlStatusModel = {
  lightBright: 0,
  pumpSpeed: 0,
  valveState: false
};

export const execResult: ExecResultModel = {
  code: 200,
  message: "success",
  command: ""
};

const defaultThresholdConfig: ThresholdConfigModel = {
  items: [
    { metric: 'temp', minValue: 18, maxValue: 30, hysteresis: 0.5 },
    { metric: 'airHum', minValue: 40, maxValue: 70, hysteresis: 2 },
    { metric: 'light', minValue: 300, maxValue: 800, hysteresis: 20 },
    { metric: 'co2', minValue: 400, maxValue: 1200, hysteresis: 50 },
    { metric: 'soilHum', minValue: 30, maxValue: 60, hysteresis: 2 },
    { metric: 'ph', minValue: 6.5, maxValue: 7.5, hysteresis: 0.1 }
  ]
};

let thresholdConfig: ThresholdConfigModel = defaultThresholdConfig;

export function getThresholdConfig(): ThresholdConfigModel {
  return thresholdConfig;
}

export function updateThresholdConfig(newConfig: ThresholdConfigModel): void {
  thresholdConfig = {
    items: newConfig.items.map((item: ThresholdConfigItem): ThresholdConfigItem => {
      const minValue: number = item.minValue;
      const maxValue: number = item.maxValue > minValue ? item.maxValue : minValue;
      const hysteresis: number = item.hysteresis >= 0 ? item.hysteresis : 0;
      const nextItem: ThresholdConfigItem = {
        metric: item.metric,
        minValue: minValue,
        maxValue: maxValue,
        hysteresis: hysteresis
      };
      return nextItem;
    })
  };
}


let mqttClient: MqttClient | null = null;
let isConnected = false;
let deviceOnline = false;
let deviceStatusTime = '';
let prefInstance: preferences.Preferences | null = null;
let sensorDataListener: SensorDataListener | undefined = undefined;
let deviceStatusListeners: Set<DeviceStatusListener> = new Set<DeviceStatusListener>();
let deviceControlStateListeners: Set<DeviceControlStateListener> = new Set<DeviceControlStateListener>();
let deviceSwitchReportListeners: Set<DeviceSwitchReportListener> = new Set<DeviceSwitchReportListener>();
let weeklyReportListeners: Set<WeeklyReportListener> = new Set<WeeklyReportListener>();
let aiAnswerListeners: Set<AiAnswerListener> = new Set<AiAnswerListener>();

export function registerSensorDataListener(listener?: SensorDataListener): void {
  sensorDataListener = listener;
}

export function registerDeviceStatusListener(listener?: DeviceStatusListener): void {
  if (listener) {
    deviceStatusListeners.add(listener);
  }
}

export function registerDeviceControlStateListener(listener?: DeviceControlStateListener): void {
  if (listener) {
    deviceControlStateListeners.add(listener);
  }
}

export function unregisterDeviceStatusListener(listener: DeviceStatusListener): void {
  deviceStatusListeners.delete(listener);
}

export function unregisterDeviceControlStateListener(listener: DeviceControlStateListener): void {
  deviceControlStateListeners.delete(listener);
}

export function registerDeviceSwitchReportListener(listener: DeviceSwitchReportListener): void {
  if (listener) {
    deviceSwitchReportListeners.add(listener);
  }
}

export function unregisterDeviceSwitchReportListener(listener: DeviceSwitchReportListener): void {
  deviceSwitchReportListeners.delete(listener);
}

export function registerWeeklyReportListener(listener: WeeklyReportListener): void {
  weeklyReportListeners.add(listener);
}

export function unregisterWeeklyReportListener(listener: WeeklyReportListener): void {
  weeklyReportListeners.delete(listener);
}

export function registerAiAnswerListener(listener: AiAnswerListener): void {
  aiAnswerListeners.add(listener);
}

export function unregisterAiAnswerListener(listener: AiAnswerListener): void {
  aiAnswerListeners.delete(listener);
}

function notifyWeeklyReport(data: WeeklyReportModel): void {
  weeklyReportListeners.forEach((listener: WeeklyReportListener) => {
    listener(data);
  });
}

function notifyAiAnswer(data: AiAnswerModel): void {
  aiAnswerListeners.forEach((listener: AiAnswerListener) => {
    listener(data);
  });
}

function notifyDeviceSwitchReport(report: DeviceSwitchReportModel): void {
  deviceSwitchReportListeners.forEach((listener: DeviceSwitchReportListener) => {
    listener(report);
  });
}

function notifyDeviceStatusChanged(online: boolean, time?: string): void {
  deviceStatusListeners.forEach((listener: DeviceStatusListener) => {
    listener(online, time);
  });
}

// =====================  初始化本地缓存（断连时保存指令） =====================
async function initCache(context: common.Context): Promise<void> {
  try {
    prefInstance = await preferences.getPreferences(context, 'agri_mqtt_cache');
    hilog.info(0x0000, 'TransportCore', '本地缓存初始化成功');
  } catch (err) {
    const errMsg = err instanceof Error ? err.message : String(err);
    hilog.error(0x0000, 'TransportCore', `缓存初始化失败：${errMsg}`);
    throw new Error(errMsg); // 包装成 Error 再抛出
  }
}

// ===================== 初始化MQTT连接 =====================
export function initMqttConnection(context: common.Context, onSuccess?: () => void, onFail?: (msg: string) => void): void {
  initCache(context).then(() => {
    try {
      // 清理旧连接
      if (mqttClient) {
        mqttClient.disconnect().catch((_e: BusinessError) => {});
        mqttClient = null;
      }
      isConnected = false;

      const clientOpts: MqttClientOptions = {
        url: `tcp://${SERVER_CONFIG.mqttIP}:${SERVER_CONFIG.mqttPort}`,
        clientId: `agri_app_${Date.now()}`,
        persistenceType: 1
      };
      mqttClient = MqttAsync.createMqtt(clientOpts);

      // 消息到达回调
      mqttClient.messageArrived((err: Error, msg: MqttMessage) => {
        if (err) {
          hilog.error(0x0000, 'TransportCore', `messageArrived error: ${err.message}`);
          return;
        }
        const topic = msg.topic;
        const dataStr = msg.payload ?? '';
        hilog.info(0x0000, 'TransportCore', `收到消息（主题：${topic}）：${dataStr}`);
        if (topic === MQTT_TOPICS.sensorData) {
          parseSensorData(dataStr);
        } else if (topic === MQTT_TOPICS.execResult) {
          parseExecResult(dataStr);
        } else if (topic === MQTT_TOPICS.deviceStatus) {
          parseDeviceStatus(dataStr);
        } else if (topic === MQTT_TOPICS.deviceSwitchStatus) {
          parseDeviceSwitchStatus(dataStr);
        } else if (topic === MQTT_TOPICS.weeklyReport) {
          parseWeeklyReportPayload(dataStr);
        } else if (topic === MQTT_TOPICS.aiAnswer) {
          parseAiAnswerPayload(dataStr);
        }
      });

      const connectOpts: MqttConnectOptions = {
        userName: SERVER_CONFIG.mqttUsername,
        password: SERVER_CONFIG.mqttPassword,
        keepAliveInterval: 60,
        cleanSession: false
      };

      mqttClient.connect(connectOpts).then((_res: MqttResponse) => {
        isConnected = true;
        hilog.info(0x0000, 'TransportCore', `✅ MQTT连接成功（${SERVER_CONFIG.mqttIP}:${SERVER_CONFIG.mqttPort}）`);
        onSuccess?.();

        // 订阅 result：收服务器返回的查询数据（序号4）
        if (mqttClient) {
          const subOpts1: MqttSubscribeOptions = { topic: MQTT_TOPICS.sensorData, qos: 1 };
          const subOpts2: MqttSubscribeOptions = { topic: MQTT_TOPICS.execResult, qos: 1 };
          const subOpts3: MqttSubscribeOptions = { topic: MQTT_TOPICS.deviceStatus, qos: 1 };
          const subOpts4: MqttSubscribeOptions = { topic: MQTT_TOPICS.controlCmd, qos: 1 };
          const subOpts5: MqttSubscribeOptions = { topic: MQTT_TOPICS.deviceSwitchStatus, qos: 1 };
          const subOpts6: MqttSubscribeOptions = { topic: MQTT_TOPICS.weeklyReport, qos: 1 };
          const subOpts7: MqttSubscribeOptions = { topic: MQTT_TOPICS.aiAnswer, qos: 1 };
          mqttClient.subscribe(subOpts1).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（收数据）：${MQTT_TOPICS.sensorData}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅传感器主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts2).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（执行结果）：${MQTT_TOPICS.execResult}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅执行结果主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts3).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（设备状态）：${MQTT_TOPICS.deviceStatus}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅设备状态主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts4).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（控制主题）：${MQTT_TOPICS.controlCmd}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅控制主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts5).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（设备开关状态）：${MQTT_TOPICS.deviceSwitchStatus}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅设备开关状态主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts6).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（周报分析）：${MQTT_TOPICS.weeklyReport}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅周报主题失败：${err.message} (code=${err.code})`);
          });
          mqttClient.subscribe(subOpts7).then(() => {
            hilog.info(0x0000, 'TransportCore', `已订阅（AI回答）：${MQTT_TOPICS.aiAnswer}`);
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `订阅AI回答主题失败：${err.message} (code=${err.code})`);
          });

          // 序号3：向 query 发查询请求，触发服务器从数据库查并往 result 推
          const requestPayload = JSON.stringify({
            device_id: SERVER_CONFIG.deviceId,
            action: 'get_latest'
          });
          mqttClient.publish({
            topic: MQTT_TOPICS.queryRequest,
            payload: requestPayload,
            qos: 1
          }).then(() => {
            hilog.info(0x0000, 'TransportCore', '已向 query 发送查询请求，等待 result 返回数据');
          }).catch((err: MqttBusinessError) => {
            hilog.error(0x0000, 'TransportCore', `发送查询请求失败：${err.message ?? ''} (code=${err.code ?? ''})`);
          });
        }

        uploadCachedCommands();
      }).catch((err: MqttBusinessError) => {
        isConnected = false;
        const errMsg = `❌ MQTT连接失败：${err.message ?? String(err)}${err.code ? ` (code=${err.code})` : ''}`;
        hilog.error(0x0000, 'TransportCore', errMsg);
        onFail?.(errMsg);
      });
    } catch (err) {
      isConnected = false;
      const anyErr = err as MqttBusinessError;
      const errMsg = `❌ MQTT初始化异常：${anyErr.message ?? String(err)}${anyErr.code ? ` (code=${anyErr.code})` : ''}`;
      hilog.error(0x0000, 'TransportCore', errMsg);
      if (anyErr.stack) hilog.error(0x0000, 'TransportCore', `stack: ${anyErr.stack}`);
      onFail?.(errMsg);
    }
  }).catch((err: Error) => {
    // 捕获缓存初始化失败的错误
    const errMsg = `❌ 缓存初始化失败，终止MQTT连接：${err instanceof Error ? err.message : String(err)}`;
    hilog.error(0x0000, 'TransportCore', errMsg);
    onFail?.(errMsg);
  });
}

// =====================  解析 result 主题的 JSON（与服务器约定一致） =====================
// 成功: { code: 200, data: { device_id, temp, hum, co2, ph, soilMoist, light, report_time } }
// 失败: { code: 500, msg: "查询失败: 无该设备数据" }
function parseSensorData(dataStr: string): void {
  try {
    const root = JSON.parse(dataStr) as SensorResponsePayload;
    if (root.code !== 200) {
      hilog.warn(0x0000, 'TransportCore', `result 失败/无数据: code=${root.code}, msg=${root.msg ?? ''}`);
      return;
    }
    const d = root.data ?? {};
    sensorData.temperature = d.temp ?? 0;
    sensorData.humidity = d.hum ?? 0;
    sensorData.co2 = d.co2 ?? 0;
    sensorData.ph = d.ph ?? 0;
    sensorData.soilMoist = d.soilMoist ?? 0;
    sensorData.light = d.light ?? 0;
    sensorData.reportTime = d.report_time ?? '';
    sensorDataListener?.(sensorData);
    hilog.info(0x0000, 'TransportCore',
      `✅ 传感器数据更新: T=${sensorData.temperature} H=${sensorData.humidity} CO2=${sensorData.co2} PH=${sensorData.ph} 土湿=${sensorData.soilMoist} 光照=${sensorData.light} 时间=${sensorData.reportTime}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析传感器数据失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

// ==================== 解析执行结果 =====================
function parseExecResult(dataStr: string): void {
  try {
    const data = JSON.parse(dataStr) as ExecResultPayload;
    execResult.code = data.code || 200;
    execResult.message = data.status || "success";
    execResult.command = data.command || "";
    hilog.info(0x0000, 'TransportCore', `✅ 执行结果更新：${execResult.command} → ${execResult.message}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析执行结果失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

function parseDeviceStatus(dataStr: string): void {
  try {
    const data = JSON.parse(dataStr) as DeviceStatusPayload;
    // 客户端兜底：如果服务端误把具体设备开关状态发到了 agri/app/device_status，
    // 这里要避免把 status=on/off 误判为主机离线，并转交给设备开关解析。
    const typeStr: string = typeof data.type === 'string' ? data.type.toLowerCase() : '';
    const statusStr: string = typeof data.status === 'string' ? data.status.toLowerCase() : '';
    const hasDeviceType: boolean = typeof data.device_type === 'string' && data.device_type.length > 0;
    if (typeStr === 'device_switch_status' || hasDeviceType || statusStr === 'on' || statusStr === 'off') {
      parseDeviceSwitchStatus(dataStr);
      return;
    }
    let resolvedOnline: boolean;
    let resolvedTime: string;
    if (typeof data.online === 'boolean') {
      resolvedOnline = data.online;
      resolvedTime = data.hardware_report_time ?? data.server_forward_time ?? data.time ?? '';
    } else if (typeof data.status === 'string') {
      resolvedOnline = data.status.toLowerCase() === 'online';
      resolvedTime = data.hardware_report_time ?? data.server_forward_time ?? data.time ?? '';
    } else {
      hilog.warn(0x0000, 'TransportCore', `设备状态消息缺少 online 或 status 字段: ${dataStr}`);
      return;
    }
    deviceOnline = resolvedOnline;
    deviceStatusTime = resolvedTime;
    notifyDeviceStatusChanged(deviceOnline, deviceStatusTime);
    hilog.info(0x0000, 'TransportCore', `✅ 设备状态更新：online=${deviceOnline}${resolvedTime ? ` time=${resolvedTime}` : ''}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析设备状态失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

function parseDeviceSwitchStatus(dataStr: string): void {
  try {
    const data = JSON.parse(dataStr) as DeviceSwitchStatusPayload;
    const deviceTypeRaw: string = typeof data.device_type === 'string' ? data.device_type.toLowerCase() : '';
    const statusRaw: string = typeof data.status === 'string' ? data.status.toLowerCase() : '';
    if (!deviceTypeRaw || !statusRaw) {
      hilog.warn(0x0000, 'TransportCore', `设备开关状态消息缺少 device_type 或 status 字段: ${dataStr}`);
      return;
    }
    if (deviceTypeRaw !== 'light' && deviceTypeRaw !== 'pump' && deviceTypeRaw !== 'valve') {
      hilog.warn(0x0000, 'TransportCore', `设备开关状态消息 device_type 非法: ${dataStr}`);
      return;
    }
    if (statusRaw !== 'on' && statusRaw !== 'off') {
      hilog.warn(0x0000, 'TransportCore', `设备开关状态消息 status 非法: ${dataStr}`);
      return;
    }
    const deviceType: DeviceType = deviceTypeRaw as DeviceType;
    const status: DeviceAction = statusRaw as DeviceAction;
    const isOn: boolean = status === 'on';
    if (deviceType === 'light') {
      controlStatus.lightBright = isOn ? 100 : 0;
    } else if (deviceType === 'pump') {
      controlStatus.pumpSpeed = isOn ? 100 : 0;
    } else if (deviceType === 'valve') {
      controlStatus.valveState = isOn;
    }
    notifyDeviceControlStateChanged();
    notifyDeviceSwitchReport({
      device_type: deviceType,
      status: status,
      hardware_report_time: data.hardware_report_time
    });
    const timeInfo: string = data.hardware_report_time ?? data.server_forward_time ?? '';
    hilog.info(0x0000, 'TransportCore', `✅ 设备开关状态更新：type=${deviceType} status=${status}${timeInfo ? ` time=${timeInfo}` : ''}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析设备开关状态失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

function parseWeeklyReportPayload(dataStr: string): void {
  try {
    const data = JSON.parse(dataStr) as WeeklyReportPayload;
    const model: WeeklyReportModel = {
      code: typeof data.code === 'number' ? data.code : 0,
      device_id: typeof data.device_id === 'string' ? data.device_id : '',
      report: typeof data.report === 'string' ? data.report : '',
      timestamp: typeof data.timestamp === 'string' ? data.timestamp : '',
      data_period: typeof data.data_period === 'string' ? data.data_period : ''
    };
    notifyWeeklyReport(model);
    hilog.info(0x0000, 'TransportCore',
      `✅ 周报已解析：code=${model.code} period=${model.data_period}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析周报消息失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

function parseAiAnswerPayload(dataStr: string): void {
  try {
    const data = JSON.parse(dataStr) as AiAnswerPayload;
    const model: AiAnswerModel = {
      code: typeof data.code === 'number' ? data.code : 0,
      device_id: typeof data.device_id === 'string' ? data.device_id : '',
      question: typeof data.question === 'string' ? data.question : '',
      answer: typeof data.answer === 'string' ? data.answer : '',
      timestamp: typeof data.timestamp === 'string' ? data.timestamp : ''
    };
    notifyAiAnswer(model);
    hilog.info(0x0000, 'TransportCore',
      `✅ AI回答已解析：code=${model.code} device=${model.device_id} time=${model.timestamp}`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `解析AI回答消息失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

function notifyDeviceControlStateChanged(): void {
  const nextState: ControlStatusModel = {
    lightBright: controlStatus.lightBright,
    pumpSpeed: controlStatus.pumpSpeed,
    valveState: controlStatus.valveState
  };
  deviceControlStateListeners.forEach((listener: DeviceControlStateListener) => {
    listener(nextState);
  });
}

// =====================  发送控制指令 =====================
export function sendControlCommand(light: number, pump: number, valve: boolean) {
  // 校验参数
  const lightVal = Math.max(0, Math.min(100, light));
  const pumpVal = Math.max(0, Math.min(100, pump));
  const valveVal = valve ? 1 : 0;

  const cmd: ControlCommandPayload = {
    light: lightVal,
    pump: pumpVal,
    valve: valveVal,
    device_id: SERVER_CONFIG.deviceId,
    timestamp: Date.now()
  };
  const cmdStr = JSON.stringify(cmd);

  if (isConnected && mqttClient) {
    const pubOpts: MqttPublishOptions = {
      topic: MQTT_TOPICS.controlCmd,
      payload: cmdStr,
      qos: 1
    };
    mqttClient.publish(pubOpts).then(() => {
      hilog.info(0x0000, 'TransportCore', `✅ 发送控制指令成功：${cmdStr}`);
    }).catch((err: MqttBusinessError) => {
      hilog.error(0x0000, 'TransportCore', `发送指令失败：${err.message} (code=${err.code})`);
      cacheControlCommand(cmdStr);
    });
  } else {
    hilog.warn(0x0000, 'TransportCore', '❌ MQTT未连接，缓存控制指令');
    cacheControlCommand(cmdStr);
  }
}

export function sendDeviceActionCommand(
  deviceType: DeviceType,
  action: DeviceAction,
  onSuccess?: () => void,
  onFail?: (msg: string) => void
): void {
  const cmd: DeviceActionPayload = {
    device_type: deviceType,
    action: action
  };
  const cmdStr: string = JSON.stringify(cmd);
  if (!isConnected || !mqttClient) {
    const errMsg: string = 'MQTT未连接，无法发送设备控制指令';
    hilog.warn(0x0000, 'TransportCore', `❌ ${errMsg}: ${cmdStr}`);
    onFail?.(errMsg);
    return;
  }
  const pubOpts: MqttPublishOptions = {
    topic: MQTT_TOPICS.controlCmd,
    payload: cmdStr,
    qos: 1
  };
  mqttClient.publish(pubOpts).then(() => {
    hilog.info(0x0000, 'TransportCore', `✅ 发送设备指令成功：topic=${MQTT_TOPICS.controlCmd} payload=${cmdStr}`);
    onSuccess?.();
  }).catch((err: MqttBusinessError) => {
    const errMsg: string = `发送设备指令失败：${err.message ?? ''} (code=${err.code ?? ''})`;
    hilog.error(0x0000, 'TransportCore', errMsg);
    onFail?.(errMsg);
  });
}

/** 测试用：发送固定控制指令（灯光50 水泵50 阀门开），供 Index 等页面调用 */
export function sendTestCommand(): void {
  sendControlCommand(50, 50, true);
}

export function requestLatestSensorData(): void {
  if (!isConnected || !mqttClient) {
    hilog.warn(0x0000, 'TransportCore', 'MQTT未连接，无法请求最新传感器数据');
    return;
  }
  const requestPayload: string = JSON.stringify({
    device_id: SERVER_CONFIG.deviceId,
    action: 'get_latest'
  });
  mqttClient.publish({
    topic: MQTT_TOPICS.queryRequest,
    payload: requestPayload,
    qos: 1
  }).then(() => {
    hilog.info(0x0000, 'TransportCore', '已发送最新传感器数据查询请求');
  }).catch((err: MqttBusinessError) => {
    hilog.error(0x0000, 'TransportCore', `请求最新传感器数据失败：${err.message ?? ''} (code=${err.code ?? ''})`);
  });
}

export function sendAiQuestion(
  question: string,
  timestamp: string,
  onSuccess?: () => void,
  onFail?: (msg: string) => void
): void {
  const text: string = question.trim();
  if (text.length === 0) {
    onFail?.('问题不能为空');
    return;
  }
  if (!isConnected || !mqttClient) {
    const errMsg: string = 'MQTT未连接，无法发送AI问题';
    hilog.warn(0x0000, 'TransportCore', errMsg);
    onFail?.(errMsg);
    return;
  }
  const payload: AiQuestionPayload = {
    device_id: SERVER_CONFIG.deviceId,
    question: text,
    timestamp: timestamp
  };
  const payloadStr: string = JSON.stringify(payload);
  const pubOpts: MqttPublishOptions = {
    topic: MQTT_TOPICS.aiQuestion,
    payload: payloadStr,
    qos: 1
  };
  mqttClient.publish(pubOpts).then(() => {
    hilog.info(0x0000, 'TransportCore', `✅ AI问题发送成功：${payloadStr}`);
    onSuccess?.();
  }).catch((err: MqttBusinessError) => {
    const errMsg: string = `发送AI问题失败：${err.message ?? ''} (code=${err.code ?? ''})`;
    hilog.error(0x0000, 'TransportCore', errMsg);
    onFail?.(errMsg);
  });
}

// =====================  离线缓存控制指令 =====================
async function cacheControlCommand(cmdStr: string): Promise<void> {
  if (!prefInstance) {
    hilog.warn(0x0000, 'TransportCore', '缓存未初始化，跳过指令缓存');
    return;
  }
  try {
    const cached = await prefInstance.get('offline_cmds', '[]') as string;
    const cmdList = JSON.parse(cached) as string[];
    cmdList.push(cmdStr);
    if (cmdList.length > 20) cmdList.shift(); // 最多缓存20条
    await prefInstance.put('offline_cmds', JSON.stringify(cmdList));
    await prefInstance.flush();
    hilog.info(0x0000, 'TransportCore', `✅ 离线指令缓存成功，共${cmdList.length}条`);
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `缓存指令失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

// =====================  重连后上传缓存指令 =====================
async function uploadCachedCommands(): Promise<void> {
  if (!isConnected || !mqttClient || !prefInstance) return;
  try {
    const cached = await prefInstance.get('offline_cmds', '[]') as string;
    const cmdList = JSON.parse(cached) as string[];
    if (cmdList.length === 0) return;

    // 循环发布（单条失败不中断）
    for (let cmdStr of cmdList) {
      try {
        const pubOpts: MqttPublishOptions = {
          topic: MQTT_TOPICS.controlCmd,
          payload: cmdStr,
          qos: 1
        };
        await mqttClient.publish(pubOpts);
        hilog.info(0x0000, 'TransportCore', `✅ 重连后上传缓存指令：${cmdStr}`);
      } catch (err) {
        hilog.error(0x0000, 'TransportCore', `上传单条缓存指令失败：${err instanceof Error ? err.message : String(err)}`);
      }
    }

    // 清空缓存
    await prefInstance.put('offline_cmds', '[]');
    await prefInstance.flush();
  } catch (err) {
    hilog.error(0x0000, 'TransportCore', `上传缓存指令失败：${err instanceof Error ? err.message : String(err)}`);
  }
}

// =====================  获取MQTT连接状态 =====================
export function getMqttStatus(): boolean {
  return isConnected;
}

export function getDeviceOnlineStatus(): boolean {
  return deviceOnline;
}

export function getDeviceStatusTime(): string {
  return deviceStatusTime;
}

export function getDeviceControlState(): ControlStatusModel {
  return {
    lightBright: controlStatus.lightBright,
    pumpSpeed: controlStatus.pumpSpeed,
    valveState: controlStatus.valveState
  };
}

// =====================  断开MQTT连接 =====================
export function disconnectMqtt(): void {
  isConnected = false;
  deviceOnline = false;
  notifyDeviceStatusChanged(deviceOnline, deviceStatusTime);
  if (mqttClient) {
    mqttClient.disconnect().then(() => {
      hilog.info(0x0000, 'TransportCore', '✅ MQTT已断开');
    }).catch((err: MqttBusinessError) => {
      hilog.error(0x0000, 'TransportCore', `断开MQTT失败：${err.message} (code=${err.code})`);
    });
    mqttClient = null;
  }
}