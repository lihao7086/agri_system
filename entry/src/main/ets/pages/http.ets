import http from '@ohos.net.http';
import hilog from '@ohos.hilog';
import { BusinessError } from '@ohos.base';

export interface HttpServerConfig {
  baseUrl: string;
  deviceId: string;
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

export interface AvgItemModel {
  date: string;
  tempAvg: number;
  humAvg: number;
  co2Avg: number;
  phAvg: number;
  soilMoistAvg: number;
  lightAvg: number;
}

export interface SevenDaysAvgModel {
  deviceId: string;
  timeRange: string;
  daysCount: number;
  avgList: AvgItemModel[];
  timestamp: string;
}

interface HealthResponsePayload {
  code?: number;
  msg?: string;
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

interface SensorResponsePayload {
  code?: number;
  msg?: string;
  data?: SensorDataPayload;
}

interface AvgItemPayload {
  date?: string;
  temp_avg?: number;
  hum_avg?: number;
  co2_avg?: number;
  ph_avg?: number;
  soilMoist_avg?: number;
  light_avg?: number;
}

interface SevenDaysAvgDataPayload {
  device_id?: string;
  time_range?: string;
  days_count?: number;
  avg_list?: AvgItemPayload[];
}

interface SevenDaysAvgResponsePayload {
  code?: number;
  msg?: string;
  data?: SevenDaysAvgDataPayload;
  timestamp?: string;
}

interface HttpPerfSampleItem {
  costMs: number;
  timeout: boolean;
}

interface HttpPerfWindowStat {
  name: string;
  windowSize: number;
  samples: HttpPerfSampleItem[];
}

export const HTTP_SERVER_CONFIG: HttpServerConfig = {
  baseUrl: 'http://115.175.12.81:8080',
  deviceId: 'hi3861_001'
};
const HTTP_PERF_WINDOW_SIZE: number = 50;
let httpPerfWindow: HttpPerfWindowStat = {
  name: 'HTTP-GET',
  windowSize: HTTP_PERF_WINDOW_SIZE,
  samples: []
};

function buildUrl(path: string, query: string): string {
  if (query.length === 0) {
    return `${HTTP_SERVER_CONFIG.baseUrl}${path}`;
  }
  return `${HTTP_SERVER_CONFIG.baseUrl}${path}?${query}`;
}

function getNowTimeText(): string {
  const now: Date = new Date();
  const year: number = now.getFullYear();
  const month: string = String(now.getMonth() + 1).padStart(2, '0');
  const day: string = String(now.getDate()).padStart(2, '0');
  const hour: string = String(now.getHours()).padStart(2, '0');
  const minute: string = String(now.getMinutes()).padStart(2, '0');
  const second: string = String(now.getSeconds()).padStart(2, '0');
  const ms: string = String(now.getMilliseconds()).padStart(3, '0');
  return `${year}-${month}-${day} ${hour}:${minute}:${second}.${ms}`;
}

function recordHttpPerfSample(costMs: number, timeout: boolean): void {
  const sample: HttpPerfSampleItem = {
    costMs: costMs,
    timeout: timeout
  };
  httpPerfWindow.samples.push(sample);
  if (httpPerfWindow.samples.length > httpPerfWindow.windowSize) {
    httpPerfWindow.samples.shift();
  }
  const count: number = httpPerfWindow.samples.length;
  if (count <= 0) {
    return;
  }
  let total: number = 0;
  let timeoutCount: number = 0;
  const costList: number[] = [];
  for (let i: number = 0; i < count; i++) {
    total = total + httpPerfWindow.samples[i].costMs;
    if (httpPerfWindow.samples[i].timeout) {
      timeoutCount = timeoutCount + 1;
    }
    costList.push(httpPerfWindow.samples[i].costMs);
  }
  costList.sort((a: number, b: number): number => a - b);
  const avg: number = total / count;
  const p95Index: number = Math.max(0, Math.ceil(count * 0.95) - 1);
  const p95: number = costList[p95Index];
  const timeoutRate: number = (timeoutCount * 100) / count;
  hilog.info(0x0000, 'HttpData',
    `📊 ${httpPerfWindow.name} 最近${count}/${httpPerfWindow.windowSize}次: avg=${avg.toFixed(1)}ms p95=${p95}ms timeoutRate=${timeoutRate.toFixed(1)}%`);
}

function doGet(url: string): Promise<string> {
  return new Promise((resolve: (value: string) => void, reject: (reason?: Error) => void) => {
    const req: http.HttpRequest = http.createHttp();
    const requestStartAt: number = Date.now();
    const requestStartText: string = getNowTimeText();
    hilog.info(0x0000, 'HttpData', `开始HTTP请求: ${url} | 请求时间=${requestStartText}`);
    req.request(url, {
      method: http.RequestMethod.GET,
      expectDataType: http.HttpDataType.STRING,
      connectTimeout: 3000,
      readTimeout: 3000,
      header: {
        'Content-Type': 'application/json'
      }
    }).then((resp: http.HttpResponse) => {
      const responseAt: number = Date.now();
      const responseText: string = getNowTimeText();
      const costMs: number = responseAt - requestStartAt;
      recordHttpPerfSample(costMs, false);
      hilog.info(0x0000, 'HttpData',
        `HTTP响应码: ${resp.responseCode} | 接收时间=${responseText} | 耗时=${costMs}ms`);
      if (resp.responseCode !== 200) {
        req.destroy();
        reject(new Error(`HTTP状态异常: ${resp.responseCode}`));
        return;
      }
      const text: string = typeof resp.result === 'string' ? resp.result : JSON.stringify(resp.result);
      hilog.info(0x0000, 'HttpData', `HTTP响应体: ${text}`);
      req.destroy();
      resolve(text);
    }).catch((err: BusinessError) => {
      const failAt: number = Date.now();
      const failText: string = getNowTimeText();
      const costMs: number = failAt - requestStartAt;
      const errMsgLower: string = (err.message ?? '').toLowerCase();
      const timeoutLike: boolean = errMsgLower.includes('timeout') || errMsgLower.includes('timed out');
      recordHttpPerfSample(costMs, timeoutLike);
      req.destroy();
      hilog.error(0x0000, 'HttpData',
        `HTTP请求失败: ${err.message} code=${err.code} | 失败时间=${failText} | 耗时=${costMs}ms`);
      reject(new Error(`HTTP GET失败: ${err.message} code=${err.code}`));
    });
  });
}

export async function queryLatestSensorData(deviceId: string): Promise<SensorDataModel> {
  const query: string = `device_id=${encodeURIComponent(deviceId)}`;
  const url: string = buildUrl('/api/get_latest_data', query);
  const text: string = await doGet(url);
  const root: SensorResponsePayload = JSON.parse(text) as SensorResponsePayload;

  if (root.code !== 200 || root.data === undefined) {
    hilog.error(0x0000, 'HttpData', `查询最新数据失败: code=${root.code} msg=${root.msg ?? ''}`);
    throw new Error(`查询最新数据失败: code=${root.code} msg=${root.msg ?? ''}`);
  }

  const d: SensorDataPayload = root.data;
  hilog.info(0x0000, 'HttpData',
    `最新数据解析成功: T=${d.temp ?? 0} H=${d.hum ?? 0} CO2=${d.co2 ?? 0} PH=${d.ph ?? 0} 土湿=${d.soilMoist ?? 0} 光照=${d.light ?? 0}`);
  return {
    temperature: d.temp ?? 0,
    humidity: d.hum ?? 0,
    light: d.light ?? 0,
    co2: d.co2 ?? 0,
    ph: d.ph ?? 0,
    soilMoist: d.soilMoist ?? 0,
    reportTime: d.report_time ?? ''
  };
}

/** 查询近 7 天每日平均值：GET /api/get_7days_avg?device_id=xxx */
export async function query7DaysAvgData(deviceId: string): Promise<SevenDaysAvgModel> {
  const query: string = `device_id=${encodeURIComponent(deviceId)}`;
  const url: string = buildUrl('/api/get_7days_avg', query);
  const text: string = await doGet(url);
  const root: SevenDaysAvgResponsePayload = JSON.parse(text) as SevenDaysAvgResponsePayload;

  if (root.code !== 200 || root.data === undefined) {
    hilog.error(0x0000, 'HttpData', `查询7天均值失败: code=${root.code} msg=${root.msg ?? ''}`);
    throw new Error(`查询7天均值失败: code=${root.code} msg=${root.msg ?? ''}`);
  }

  const data: SevenDaysAvgDataPayload = root.data;
  const avgListPayload: AvgItemPayload[] = data.avg_list ?? [];
  const avgList: AvgItemModel[] = avgListPayload.map((item: AvgItemPayload): AvgItemModel => {
    return {
      date: item.date ?? '--',
      tempAvg: item.temp_avg ?? 0,
      humAvg: item.hum_avg ?? 0,
      co2Avg: item.co2_avg ?? 0,
      phAvg: item.ph_avg ?? 0,
      soilMoistAvg: item.soilMoist_avg ?? 0,
      lightAvg: item.light_avg ?? 0
    };
  });

  hilog.info(0x0000, 'HttpData',
    `7天均值解析成功: device=${data.device_id ?? ''} days=${data.days_count ?? 0} points=${avgList.length}`);

  return {
    deviceId: data.device_id ?? '',
    timeRange: data.time_range ?? '',
    daysCount: data.days_count ?? avgList.length,
    avgList: avgList,
    timestamp: root.timestamp ?? ''
  };
}