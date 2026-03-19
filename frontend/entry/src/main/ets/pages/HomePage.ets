import common from '@ohos.app.ability.common';
import preferences from '@ohos.data.preferences';
import hilog from '@ohos.hilog';
import {
  ControlStatusModel,
  DeviceStatusListener,
  disconnectMqtt as mqttDisconnect,
  DeviceControlStateListener,
  getDeviceControlState,
  getDeviceOnlineStatus,
  getDeviceStatusTime,
  initMqttConnection as mqttInit,
  registerDeviceControlStateListener,
  registerDeviceStatusListener,
  unregisterDeviceControlStateListener,
  unregisterDeviceStatusListener,
  ThresholdConfigItem,
  ThresholdConfigModel,
  ThresholdMetric,
  getThresholdConfig,
  updateThresholdConfig
} from './mqtt';
import {
  HTTP_SERVER_CONFIG,
  queryLatestSensorData,
  SensorDataModel
} from './http';

interface EnvItem {
  title: string;
  value: number;
  unit: string;
  min: number;
  max: number;
  icon: ResourceStr;
}

interface DeviceConnInfo {
  deviceId: string;
  status: '连接' | '离线';
  lastHeartbeat: string;
}

interface QuickControlItem {
  name: string;
  isRunning: boolean;
  runningMinutes: number;
  executeResult: string; // 先写“未执行”
}

type WarningLevel = 'normal' | 'low' | 'high';

interface WarningStateItem {
  metric: ThresholdMetric;
  level: WarningLevel;
}

@Component
export struct HomePage {
  pathStack: NavPathStack = new NavPathStack();
  private httpQueryTimer: number = -1;
  private readonly onDeviceStatusChanged: DeviceStatusListener = (online: boolean, time?: string): void => {
    const nextConn: DeviceConnInfo = {
      deviceId: this.deviceConn.deviceId,
      status: online ? '连接' : '离线',
      lastHeartbeat: time !== undefined && time.length > 0 ? time : this.deviceConn.lastHeartbeat
    };
    this.deviceConn = nextConn;
  };
  private readonly onDeviceControlStateChanged: DeviceControlStateListener = (state: ControlStatusModel): void => {
    this.syncQuickControlsFromGlobalState(state);
  };
  @State envItems: EnvItem[] = [
    { title: '温度', value: 0, unit: '°C', min: 18, max: 30, icon: $r('app.media.temperature') },
    { title: '空气湿度', value: 0, unit: '%', min: 40, max: 70, icon: $r('app.media.humidity1') },
    { title: '光照', value: 0, unit: 'lux', min: 300, max: 800, icon: $r('app.media.sunshine') },
    { title: 'CO₂', value: 0, unit: 'ppm', min: 400, max: 800, icon: $r('app.media.co2') },
    { title: '土壤湿度', value: 0, unit: '%', min: 30, max: 60, icon: $r('app.media.humidity2') },
    { title: 'PH值', value: 0, unit: '', min: 6.5, max: 7.5, icon: $r('app.media.ph') }
  ];
  @State alerts: string[] = [];
  @State currentAlertIndex: number = 0;
  @State showDeviceDialog: boolean = false;
  @State activeDeviceIndex: number = -1;
  @State deviceConnected: boolean = false;
  @State lastQuerySuccess: boolean = false;
  // 灌溉设备状态
  @State irrigationOn: boolean = false;
  @State irrigationWorkedMinutes: number = 0;
  @State irrigationRemainMinutes: number = 0;
  // 补光设备状态
  @State lightingOn: boolean = false;
  @State lightingWorkedMinutes: number = 0;
  @State lightingRemainMinutes: number = 0;
  // 通风设备状态
  @State ventilationOn: boolean = false;
  @State ventilationWorkedMinutes: number = 0;
  @State ventilationRemainMinutes: number = 0;

  @State lastUpdateTime: string = '--';
  @State deviceConn: DeviceConnInfo = {
    deviceId: 'dev001',
    status: '离线',
    lastHeartbeat: '--'
  };
  @State quickControls: QuickControlItem[] = [
    { name: '灌溉', isRunning: false, runningMinutes: 0, executeResult: '未执行' },
    { name: '补光', isRunning: false, runningMinutes: 0, executeResult: '未执行' },
    { name: '通风', isRunning: false, runningMinutes: 0, executeResult: '未执行' }
  ];
  @State warningStates: WarningStateItem[] = [];

  aboutToAppear(): void {
    this.loadThresholdConfigFromPreferences();
    const deviceOnline: boolean = getDeviceOnlineStatus();
    const deviceStatusTime: string = getDeviceStatusTime();
    this.deviceConn = {
      deviceId: this.deviceConn.deviceId,
      status: deviceOnline ? '连接' : '离线',
      lastHeartbeat: deviceStatusTime.length > 0 ? deviceStatusTime : this.deviceConn.lastHeartbeat
    };
    this.syncQuickControlsFromGlobalState(getDeviceControlState());
    registerDeviceStatusListener(this.onDeviceStatusChanged);
    registerDeviceControlStateListener(this.onDeviceControlStateChanged);
    this.queryLatestDataAndRender();
    this.startHttpQueryLoop();
  }

  aboutToDisappear(): void {
    unregisterDeviceStatusListener(this.onDeviceStatusChanged);
    unregisterDeviceControlStateListener(this.onDeviceControlStateChanged);
    this.stopHttpQueryLoop();
  }

  build() {
    Stack() {
      Column() {
        Row() {
          Text('首页')
            .fontSize(20)
            .fontWeight(FontWeight.Bold)
            .fontColor(Color.White)

          Text(this.lastQuerySuccess ? '最近一次查询成功' : '最近一次查询失败')
            .layoutWeight(1)
            .textAlign(TextAlign.Center)
            .fontSize(14)
            .fontWeight(FontWeight.Medium)
            .fontColor(Color.White)

          Button('趋势分析')
            .height(32)
            .fontSize(12)
            .fontColor('#2E7D32')
            .backgroundColor(Color.White)
            .borderRadius(8)
            .onClick(() => {
              this.openTrendPage();
            })
        }
        .width('100%')
        .height(56)
        .padding({ left: 16, right: 16 })
        .backgroundColor('#2E7D32')
        .alignItems(VerticalAlign.Center)

        List() {
          // 环境监测板块
          ListItem() {
            Column({ space: 12 }) {
              Row() {
                Row({ space: 8 }) {
                  Image($r('app.media.env'))
                    .width(20)
                    .height(20)
                  Text('环境监测概览')
                    .fontSize(18)
                    .fontWeight(FontWeight.Medium)
                    .fontColor('#333333')
                    .alignSelf(ItemAlign.Start)
                }
                .layoutWeight(1)

                Image($r('app.media.refresh'))
                  .width(20)
                  .height(20)
                  .onClick(() => {
                    this.queryLatestDataAndRender();
                  })
              }
              .width('100%')
              .alignItems(VerticalAlign.Center)
              ForEach([0, 2, 4], (startIndex: number) => {
                Row({ space: 12 }) {
                  this.EnvCard(startIndex)
                  if (startIndex + 1 < this.envItems.length) {
                    this.EnvCard(startIndex + 1)
                  }
                }
              }, (startIndex: number) => `${startIndex}-${this.lastUpdateTime}`)
              Text(`最后更新时间：${this.lastUpdateTime}`)
                .fontSize(12)
                .fontColor('#999999')
                .alignSelf(ItemAlign.Start)
            }
            .width('100%')
            .padding(16)
            .backgroundColor('#F8F9FA')
            .borderRadius(12)
            .alignItems(HorizontalAlign.Start)
          }

          // 预警板块
          ListItem() {
            Column({ space: 8 }) {
              Row({ space: 8 }) {
                Image($r('app.media.alarm'))
                  .width(20)
                  .height(20)
                  .fillColor('#666666')
                Text('预警')
                  .fontSize(16)
                  .fontWeight(FontWeight.Medium)
                  .fontColor('#333333')
              }
              .alignItems(VerticalAlign.Center)
              .alignSelf(ItemAlign.Start)

              if (!this.lastQuerySuccess || this.alerts.length === 0) {
                Text('无')
                  .fontSize(14)
                  .fontColor('#999999')
                  .alignSelf(ItemAlign.Start)
              } else {
                Swiper() {
                  ForEach(this.alerts, (item: string) => {
                    Column({ space: 4 }) {
                      Text(item)
                        .fontSize(14)
                        .fontColor(Color.Red)
                        .alignSelf(ItemAlign.Start)
                      Text(this.getAlertSuggestion(item))
                        .fontSize(12)
                        .fontColor('#666666')
                        .alignSelf(ItemAlign.Start)
                    }
                    .width('100%')
                    .alignItems(HorizontalAlign.Start)
                  }, (item: string) => item)
                }
                .height(80)
                .autoPlay(true)
                .interval(5000)
                .loop(true)
                .indicator(true)
                .onChange((index: number) => {
                  this.currentAlertIndex = index;
                })
              }
            }
            .width('100%')
            .padding(16)
            .backgroundColor('#F8F9FA')
            .borderRadius(12)
            .alignItems(HorizontalAlign.Start)
          }

          // 快捷控制板块
          ListItem() {
            Column({ space: 12 }) {
              Row({ space: 8 }) {
                Image($r('app.media.quickcontrol'))
                  .width(20)
                  .height(20)
                Text('快捷控制')
                  .fontSize(16)
                  .fontWeight(FontWeight.Medium)
                  .fontColor('#333333')
                  .alignSelf(ItemAlign.Start)
              }

              Column({ space: 10 }) {
                ForEach(this.quickControls, (item: QuickControlItem, index: number) => {
                  this.QuickItem(item, index)
                }, (item: QuickControlItem) => item.name)
              }
              .width('100%')
            }
            .width('100%')
            .padding(16)
            .backgroundColor('#F8F9FA')
            .borderRadius(12)
            .alignItems(HorizontalAlign.Start)
          }
        }
        .listDirection(Axis.Vertical)
        .edgeEffect(EdgeEffect.Spring)
        .layoutWeight(1)
      }
      .width('100%')
      .height('100%')
      .backgroundColor('#F0F2F5')

      if (this.showDeviceDialog && this.activeDeviceIndex >= 0) {
        Column() {}
          .width('100%')
          .height('100%')
          .backgroundColor('#66000000')
          .onClick(() => {
            this.closeDeviceDialog();
          })

        this.DeviceDialog()
      }
    }
    .width('100%')
    .height('100%')
  }
  
  // 获取预警建议
  getAlertSuggestion(alertText: string): string {
    const isLow: boolean = alertText.includes('偏低') || alertText.includes('过低');
    const isHigh: boolean = alertText.includes('偏高') || alertText.includes('过高');

    // 空气温度
    if (alertText.includes('温度')) {
      if (isHigh) {
        return '建议：请开启通风或遮阳设备，必要时开启降温设施。';
      }
      if (isLow) {
        return '建议：请关闭通风并做好保温，可适当开启补光或加热设备。';
      }
    }

    // 空气湿度 / 土壤湿度
    if (alertText.includes('空气湿度')) {
      if (isLow) {
        return '建议：请适当增加灌溉或喷雾，提升空气湿度。';
      }
      if (isHigh) {
        return '建议：请降低灌溉频率，并加强通风以降低空气湿度。';
      }
    }
    if (alertText.includes('土壤湿度')) {
      if (isLow) {
        return '建议：请及时开启灌溉系统，补充土壤水分。';
      }
      if (isHigh) {
        return '建议：请暂缓灌溉并注意排水，防止根系缺氧。';
      }
    }

    // 光照
    if (alertText.includes('光照')) {
      if (isLow) {
        return '建议：请开启LED补光设备或增加采光时间。';
      }
      if (isHigh) {
        return '建议：请适当遮阴或减少直射光照，避免灼伤植株。';
      }
    }

    // CO₂ 浓度
    if (alertText.includes('CO₂') || alertText.includes('二氧化碳')) {
      if (isHigh) {
        return '建议：请加强通风换气，保持棚内空气流通。';
      }
      if (isLow) {
        return '建议：可适当增加施肥或补充二氧化碳，提高光合作用效率。';
      }
    }

    // PH 值
    if (alertText.includes('PH值')) {
      if (isLow) {
        return '建议：土壤偏酸，可适量施用石灰等碱性改良剂，逐步调高PH值。';
      }
      if (isHigh) {
        return '建议：土壤偏碱，可适量施用酸性肥料或有机质，逐步调低PH值。';
      }
    }

    return '建议：请结合实际情况，综合调整灌溉、通风、补光与施肥策略。';
  }

  getEnvStatus(item: EnvItem): string {
    if (item.value < item.min) {
      return '偏低';
    }
    if (item.value > item.max) {
      return '偏高';
    }
    return '正常';
  }

  getEnvColor(item: EnvItem): Color {
    if (item.value < item.min) {
      return Color.Blue;
    }
    if (item.value > item.max) {
      return Color.Red;
    }
    return Color.Green;
  }

  getEnvStatusByIndex(index: number): string {
    return this.getEnvStatus(this.envItems[index]);
  }

  getEnvColorByIndex(index: number): Color {
    return this.getEnvColor(this.envItems[index]);
  }

  @Builder
  EnvCard(index: number) {
    Column({ space: 8 }) {
      Text(this.envItems[index].title)
        .fontSize(12)
        .fontColor(this.lastQuerySuccess ? '#666666' : '#999999')
      Row({ space: 8 }) {
        Image(this.envItems[index].icon)
          .width(32)
          .height(32)
          .fillColor(this.lastQuerySuccess ? this.getEnvColorByIndex(index) : '#B0B0B0')
        Column({ space: 4 }) {
          if (this.lastQuerySuccess) {
            Text(`${this.envItems[index].value}${this.envItems[index].unit}`)
              .fontSize(18)
              .fontWeight(FontWeight.Medium)
              .fontColor(this.getEnvColorByIndex(index))
            Text(`(${this.getEnvStatusByIndex(index)})`)
              .fontSize(12)
              .fontColor(this.getEnvColorByIndex(index))
          } else {
            Text('暂无数据')
              .fontSize(16)
              .fontWeight(FontWeight.Medium)
              .fontColor('#999999')
            Text('请点击刷新')
              .fontSize(12)
              .fontColor('#B0B0B0')
          }
        }
      }
      .alignItems(VerticalAlign.Center)
    }
    .layoutWeight(1)
    .padding(12)
    .backgroundColor(Color.White)
    .borderRadius(8)
    .alignItems(HorizontalAlign.Start)
  }

  @Builder
  QuickItem(item: QuickControlItem, index: number) {
    Row({ space: 12 }) {
      Column({ space: 6 }) {
        Text(item.name)
          .fontSize(16)
          .fontWeight(FontWeight.Medium)
          .fontColor('#333333')

        Text(`当前状态：${item.isRunning ? '运行中' : '已关闭'}`)
          .fontSize(12)
          .fontColor(item.isRunning ? '#2E7D32' : '#999999')

        Text(`运行时长：${item.runningMinutes} min`)
          .fontSize(12)
          .fontColor('#666666')

        Text(`执行结果：${item.executeResult}`)
          .fontSize(12)
          .fontColor('#D32F2F')
      }
      .layoutWeight(1)
      .alignItems(HorizontalAlign.Start)

      Button(item.isRunning ? '关闭' : '开启')
        .width(84)
        .height(38)
        .fontSize(14)
        .fontColor(Color.White)
        .backgroundColor('#2E7D32')
        .borderRadius(10)
        .onClick(() => {
          this.openDeviceDialog(index);
        })
    }
    .width('100%')
    .padding(10)
    .backgroundColor(Color.White)
    .borderRadius(8)
    .alignItems(VerticalAlign.Center)
    .onClick(() => {
      this.openDeviceDialog(index);
    })
  }

  openDeviceDialog(index: number) {
    this.activeDeviceIndex = index;
    this.showDeviceDialog = true;
  }

  closeDeviceDialog() {
    this.showDeviceDialog = false;
  }

  openTrendPage(): void {
    this.pathStack.pushPathByName('TrendPage', null, false);
  }

  handleGatewayAction(): void {
    if (this.deviceConnected) {
      this.confirmDisconnectGateway();
      return;
    }
    this.confirmConnectGateway();
  }

  confirmConnectGateway(): void {
    AlertDialog.show({
      title: '提示',
      message: '确认连接设备吗？',
      primaryButton: {
        value: '确认',
        action: () => {
          this.initMqttConnection();
        }
      },
      secondaryButton: {
        value: '取消',
        action: () => {
          // 取消连接
        }
      }
    });
  }

  confirmDisconnectGateway(): void {
    AlertDialog.show({
      title: '提示',
      message: '确认断开设备吗？',
      primaryButton: {
        value: '确认',
        action: () => {
          this.disconnectMqtt();
        }
      },
      secondaryButton: {
        value: '取消',
        action: () => {
          // 取消断开
        }
      }
    });
  }

  initMqttConnection(): void {
    const context: common.UIAbilityContext = getContext(this) as common.UIAbilityContext;
    mqttInit(context, () => {
      this.deviceConnected = true;
      const nextConn: DeviceConnInfo = {
        deviceId: this.deviceConn.deviceId,
        status: '连接',
        lastHeartbeat: this.deviceConn.lastHeartbeat
      };
      this.deviceConn = nextConn;
    }, () => {
      this.deviceConnected = false;
      const nextConn: DeviceConnInfo = {
        deviceId: this.deviceConn.deviceId,
        status: '离线',
        lastHeartbeat: this.deviceConn.lastHeartbeat
      };
      this.deviceConn = nextConn;
    });
  }

  disconnectMqtt(): void {
    this.deviceConnected = false;
    const nextConn: DeviceConnInfo = {
      deviceId: this.deviceConn.deviceId,
      status: '离线',
      lastHeartbeat: this.deviceConn.lastHeartbeat
    };
    this.deviceConn = nextConn;
    mqttDisconnect();
  }

  startHttpQueryLoop(): void {
    this.stopHttpQueryLoop();
    this.httpQueryTimer = setInterval(() => {
      this.queryLatestDataAndRender();
    }, 10000);
  }

  stopHttpQueryLoop(): void {
    if (this.httpQueryTimer !== -1) {
      clearInterval(this.httpQueryTimer);
      this.httpQueryTimer = -1;
    }
  }

  async queryLatestDataAndRender(): Promise<void> {
    hilog.info(0x0000, 'HomePageHttp', `开始查询最新数据, deviceId=${HTTP_SERVER_CONFIG.deviceId}`);
    try {
      const data: SensorDataModel = await queryLatestSensorData(HTTP_SERVER_CONFIG.deviceId);

      this.syncEnvItemsFromMqtt(data);

      hilog.info(0x0000, 'HomePageHttp', '查询并渲染成功');
    } catch (_error) {
      const errorMsg: string = _error instanceof Error ? _error.message : String(_error);
      hilog.error(0x0000, 'HomePageHttp', `查询失败: ${errorMsg}`);

      this.lastQuerySuccess = false;
      this.lastUpdateTime = '--';
    }
  }

  syncEnvItemsFromMqtt(data: SensorDataModel): void {
    hilog.info(0x0000, 'HomePageHttp', `收到传感器数据: temp=${data.temperature}, hum=${data.humidity}, light=${data.light}`);

    const nextEnvItems: EnvItem[] = this.envItems.map((item: EnvItem, index: number): EnvItem => {
      let newValue: number = item.value;
      switch (index) {
        case 0: newValue = data.temperature; break;
        case 1: newValue = data.humidity; break;
        case 2: newValue = data.light; break;
        case 3: newValue = data.co2; break;
        case 4: newValue = data.soilMoist; break;
        case 5: newValue = data.ph; break;
        default: break;
      }
      
      const nextItem: EnvItem = {
        title: item.title,
        value: newValue,
        unit: item.unit,
        min: item.min,
        max: item.max,
        icon: item.icon
      };
      return nextItem;
    });

    this.envItems = nextEnvItems;
    this.updateAlertsFromEnv(nextEnvItems);

    this.lastQuerySuccess = true;
    this.lastUpdateTime = data.reportTime && data.reportTime.length > 0 ? data.reportTime : new Date().toLocaleTimeString();
    
    const nextConn: DeviceConnInfo = {
      deviceId: this.deviceConn.deviceId,
      status: this.deviceConnected ? '连接' : '离线',
      lastHeartbeat: this.lastUpdateTime
    };
    this.deviceConn = nextConn;
  }

  syncQuickControlsFromGlobalState(state: ControlStatusModel): void {
    this.irrigationOn = state.valveState;
    this.lightingOn = state.lightBright > 0;
    this.ventilationOn = state.pumpSpeed > 0;
    const nextControls: QuickControlItem[] = this.quickControls.map((item: QuickControlItem, index: number): QuickControlItem => {
      const nextItem: QuickControlItem = {
        name: item.name,
        isRunning: index === 0 ? this.irrigationOn : (index === 1 ? this.lightingOn : this.ventilationOn),
        runningMinutes: item.runningMinutes,
        executeResult: item.executeResult
      };
      return nextItem;
    });
    this.quickControls = nextControls;
  }

  private async loadThresholdConfigFromPreferences(): Promise<void> {
    const context: common.UIAbilityContext = getContext(this) as common.UIAbilityContext;
    try {
      const inst: preferences.Preferences = await preferences.getPreferences(context, 'agri_thresholds');
      const rawObj: Object = await inst.get('warning_thresholds', '');
      const raw: string = rawObj as string;
      if (!raw || raw.length === 0) {
        return;
      }
      const parsed = JSON.parse(raw) as ThresholdConfigModel;
      if (!parsed || !parsed.items || parsed.items.length === 0) {
        return;
      }
      updateThresholdConfig(parsed);
    } catch (_e) {
    }
  }

  private getThresholdItem(metric: ThresholdMetric): ThresholdConfigItem | undefined {
    const cfg: ThresholdConfigModel = getThresholdConfig();
    return cfg.items.find((item: ThresholdConfigItem): boolean => item.metric === metric);
  }

  private evaluateWarningLevel(
    value: number,
    config: ThresholdConfigItem,
    prevLevel: WarningLevel
  ): WarningLevel {
    const minValue: number = config.minValue;
    const maxValue: number = config.maxValue;
    const hysteresis: number = config.hysteresis;
    if (prevLevel === 'normal') {
      if (value < minValue) {
        return 'low';
      }
      if (value > maxValue) {
        return 'high';
      }
      return 'normal';
    }
    if (prevLevel === 'low') {
      if (value >= minValue + hysteresis) {
        return 'normal';
      }
      return 'low';
    }
    if (prevLevel === 'high') {
      if (value <= maxValue - hysteresis) {
        return 'normal';
      }
      return 'high';
    }
    return 'normal';
  }

  private updateAlertsFromEnv(envItems: EnvItem[]): void {
    const metrics: ThresholdMetric[] = ['temp', 'airHum', 'light', 'co2', 'soilHum', 'ph'];
    const values: number[] = [
      envItems[0]?.value ?? 0,
      envItems[1]?.value ?? 0,
      envItems[2]?.value ?? 0,
      envItems[3]?.value ?? 0,
      envItems[4]?.value ?? 0,
      envItems[5]?.value ?? 0
    ];
    const nextWarningStates: WarningStateItem[] = [];
    const nextAlerts: string[] = [];
    for (let i: number = 0; i < metrics.length; i++) {
      const metric: ThresholdMetric = metrics[i];
      const value: number = values[i];
      const config: ThresholdConfigItem | undefined = this.getThresholdItem(metric);
      if (!config) {
        continue;
      }
      const prev: WarningStateItem | undefined = this.warningStates.find((st: WarningStateItem): boolean => st.metric === metric);
      const prevLevel: WarningLevel = prev ? prev.level : 'normal';
      const level: WarningLevel = this.evaluateWarningLevel(value, config, prevLevel);
      const state: WarningStateItem = {
        metric: metric,
        level: level
      };
      nextWarningStates.push(state);
      if (level !== 'normal') {
        const alertText: string = this.buildAlertText(metric, level, value, config);
        nextAlerts.push(alertText);
      }
    }
    this.warningStates = nextWarningStates;
    this.alerts = nextAlerts;
  }

  private buildAlertText(
    metric: ThresholdMetric,
    level: WarningLevel,
    value: number,
    config: ThresholdConfigItem
  ): string {
    const direction: string = level === 'low' ? '偏低' : '偏高';
    if (metric === 'temp') {
      return `温度${direction}（当前 ${value.toFixed(1)}°C，阈值 ${config.minValue}~${config.maxValue}°C）`;
    }
    if (metric === 'airHum') {
      return `空气湿度${direction}（当前 ${value.toFixed(1)}%，阈值 ${config.minValue}~${config.maxValue}%）`;
    }
    if (metric === 'light') {
      return `光照${direction}（当前 ${value.toFixed(0)} lux，阈值 ${config.minValue}~${config.maxValue} lux）`;
    }
    if (metric === 'co2') {
      return `CO₂ 浓度${direction}（当前 ${value.toFixed(0)} ppm，阈值 ${config.minValue}~${config.maxValue} ppm）`;
    }
    if (metric === 'soilHum') {
      return `土壤湿度${direction}（当前 ${value.toFixed(1)}%，阈值 ${config.minValue}~${config.maxValue}%）`;
    }
    if (metric === 'ph') {
      return `PH值${direction}（当前 ${value.toFixed(2)}，阈值 ${config.minValue}~${config.maxValue}）`;
    }
    return '环境参数异常';
  }
  resetEnvItemsToZero(): void {
    const nextEnvItems: EnvItem[] = [];
    for (let i: number = 0; i < this.envItems.length; i++) {
      const nextItem: EnvItem = {
        title: this.envItems[i].title,
        value: 0,
        unit: this.envItems[i].unit,
        min: this.envItems[i].min,
        max: this.envItems[i].max,
        icon: this.envItems[i].icon
      };
      nextEnvItems.push(nextItem);
    }
    this.envItems = nextEnvItems;
    this.lastQuerySuccess = false;
    this.lastUpdateTime = '--';
  }

  confirmQuickControl(index: number): void {
    const current: QuickControlItem = this.quickControls[index];
    const targetRunning: boolean = !current.isRunning;
    const actionText: string = targetRunning ? '开启' : '关闭';

    AlertDialog.show({
      title: '提示',
      message: `确认${actionText}${current.name}设备吗？`,
      primaryButton: {
        value: '是',
        action: () => {
          const nextItem: QuickControlItem = {
            name: current.name,
            // 未收到后端成功回执前，不变更本地开关状态
            isRunning: current.isRunning,
            runningMinutes: current.runningMinutes,
            executeResult: '未执行'
          };
          const nextList: QuickControlItem[] = [];
          for (let i: number = 0; i < this.quickControls.length; i++) {
            nextList.push(this.quickControls[i]);
          }
          nextList[index] = nextItem;
          this.quickControls = nextList;
          this.closeDeviceDialog();
        }
      },
      secondaryButton: {
        value: '否',
        action: () => {
          // 取消操作，保持原状态
        }
      }
    });
  }

  getDeviceOn(index: number): boolean {
    if (index === 0) {
      return this.irrigationOn;
    }
    if (index === 1) {
      return this.lightingOn;
    }
    return this.ventilationOn;
  }

  setDeviceOn(index: number, value: boolean) {
    if (index === 0) {
      this.irrigationOn = value;
    } else if (index === 1) {
      this.lightingOn = value;
    } else if (index === 2) {
      this.ventilationOn = value;
    }
  }

  getDeviceWorkedMinutes(index: number): number {
    if (index === 0) {
      return this.irrigationWorkedMinutes;
    }
    if (index === 1) {
      return this.lightingWorkedMinutes;
    }
    return this.ventilationWorkedMinutes;
  }

  getDeviceRemainMinutes(index: number): number {
    if (index === 0) {
      return this.irrigationRemainMinutes;
    }
    if (index === 1) {
      return this.lightingRemainMinutes;
    }
    return this.ventilationRemainMinutes;
  }

  @Builder
  DeviceDialog() {
    Column({ space: 16 }) {
      Row({ space: 12 }) {
        Image($r('app.media.return'))
          .width(24)
          .height(24)
          .onClick(() => {
            this.closeDeviceDialog();
          })
        Text(this.quickControls[this.activeDeviceIndex].name + '设备')
          .fontSize(18)
          .fontWeight(FontWeight.Medium)
          .fontColor('#333333')
      }
      .alignItems(VerticalAlign.Center)
      .alignSelf(ItemAlign.Start)

      if (!this.getDeviceOn(this.activeDeviceIndex)) {
        Text('设备未开启')
          .fontSize(14)
          .fontColor('#666666')
          .alignSelf(ItemAlign.Start)
      } else {
        Text(`设备已运行 ${this.getDeviceWorkedMinutes(this.activeDeviceIndex)} 分钟，预计 ${this.getDeviceRemainMinutes(this.activeDeviceIndex)} 分钟后结束`)
          .fontSize(14)
          .fontColor('#666666')
          .alignSelf(ItemAlign.Start)
      }

      Button(this.getDeviceOn(this.activeDeviceIndex) ? '关闭设备' : `开启智能${this.quickControls[this.activeDeviceIndex].name}`)
        .width('100%')
        .height(48)
        .fontSize(16)
        .fontColor(Color.White)
        .backgroundColor('#2E7D32')
        .borderRadius(8)
        .onClick(() => {
          this.confirmQuickControl(this.activeDeviceIndex);
        })
    }
    .width('100%')
    .padding(16)
    .backgroundColor(Color.White)
    .borderRadius(12)
    .alignItems(HorizontalAlign.Start)
    .align(Alignment.Center)
    .onClick(() => {
      // 阻止点击弹窗内容时触发蒙层关闭
    })
  }

}