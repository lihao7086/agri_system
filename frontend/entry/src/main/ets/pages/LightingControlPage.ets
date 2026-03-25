import {
  ControlStatusModel,
  DeviceAction,
  DeviceControlStateListener,
  DeviceStatusListener,
  DeviceSwitchReportListener,
  DeviceSwitchReportModel,
  getDeviceControlState,
  getDeviceOnlineStatus,
  getDeviceStatusTime,
  getMqttStatus,
  registerDeviceControlStateListener,
  registerDeviceStatusListener,
  registerDeviceSwitchReportListener,
  unregisterDeviceControlStateListener,
  unregisterDeviceStatusListener,
  unregisterDeviceSwitchReportListener,
  sendDeviceActionCommand
} from './mqtt';

const PENDING_DEVICE_ACK_MS: number = 5000;
const MIN_MS_FOR_OPPOSITE_ACK: number = 200;

@Builder
export function LightingControlPageBuilder() {
  LightingControlPage()
}

@Component
struct LightingControlPage {
  private pendingExpectLightOn: boolean | undefined = undefined;
  private pendingTimerId: number = -1;
  private commandIssuedAt: number = 0;

  private clearLightPendingTimer(): void {
    if (this.pendingTimerId !== -1) {
      clearTimeout(this.pendingTimerId);
      this.pendingTimerId = -1;
    }
  }

  private clearLightPendingAll(): void {
    this.clearLightPendingTimer();
    this.pendingExpectLightOn = undefined;
  }

  private readonly onControlStateChanged: DeviceControlStateListener = (state: ControlStatusModel): void => {
    this.lightOn = state.lightBright > 0;
    this.lightBrightness = state.lightBright;
    if (this.pendingExpectLightOn === true && state.lightBright > 0) {
      this.clearLightPendingAll();
      this.executeResult = '开启成功（设备已上报运行）';
    } else if (this.pendingExpectLightOn === false && state.lightBright <= 0) {
      this.clearLightPendingAll();
      this.executeResult = '关闭成功（设备已上报关闭）';
    }
  };

  private readonly onSwitchReport: DeviceSwitchReportListener = (r: DeviceSwitchReportModel): void => {
    if (r.device_type !== 'light') {
      return;
    }
    if (Date.now() - this.commandIssuedAt < MIN_MS_FOR_OPPOSITE_ACK) {
      return;
    }
    if (this.pendingExpectLightOn === true && r.status === 'off') {
      this.clearLightPendingAll();
      this.executeResult = '开启失败：硬件反馈为关闭状态';
      AlertDialog.show({
        title: '提示',
        message: '开启失败：设备上报仍为关闭状态。',
        primaryButton: { value: '知道了', action: () => {} }
      });
    } else if (this.pendingExpectLightOn === false && r.status === 'on') {
      this.clearLightPendingAll();
      this.executeResult = '关闭失败：硬件反馈仍为运行状态';
      AlertDialog.show({
        title: '提示',
        message: '关闭失败：设备上报仍为运行状态。',
        primaryButton: { value: '知道了', action: () => {} }
      });
    }
  };

  private readonly onDeviceStatusChanged: DeviceStatusListener = (online: boolean, time?: string): void => {
    this.deviceOnline = online;
    if (time !== undefined) {
      this.deviceStatusTime = time;
    }
  };
  @State lightOn: boolean = false;
  @State lightRunningMinutes: number = 0;
  @State deviceConnected: boolean = false;
  @State deviceOnline: boolean = false;
  @State deviceStatusTime: string = '';
  @State executeResult: string = '未执行';
  @State lightBrightness: number = 38;
  @State smartDurationMinutes: number = 60;
  @State selectedDurationKey: string = '1h';
  @State customDurationInput: string = '';

  aboutToAppear(): void {
    const controlState: ControlStatusModel = getDeviceControlState();
    this.lightOn = controlState.lightBright > 0;
    this.lightBrightness = controlState.lightBright;
    this.deviceConnected = getMqttStatus();
    this.deviceOnline = getDeviceOnlineStatus();
    this.deviceStatusTime = getDeviceStatusTime();
    registerDeviceControlStateListener(this.onControlStateChanged);
    registerDeviceStatusListener(this.onDeviceStatusChanged);
    registerDeviceSwitchReportListener(this.onSwitchReport);
  }

  aboutToDisappear(): void {
    this.clearLightPendingAll();
    unregisterDeviceControlStateListener(this.onControlStateChanged);
    unregisterDeviceStatusListener(this.onDeviceStatusChanged);
    unregisterDeviceSwitchReportListener(this.onSwitchReport);
  }

  getLightStatusText(): string {
    return this.lightOn ? '运行中' : '已关闭';
  }

  onLightButtonClick(): void {
    const connectedNow: boolean = getMqttStatus();
    this.deviceConnected = connectedNow;
    if (!connectedNow) {
      AlertDialog.show({
        title: '提示',
        message: '控制中心未连接，无法操作设备。',
        primaryButton: { value: '知道了', action: () => {} }
      });
      return;
    }
    if (!this.deviceOnline) {
      AlertDialog.show({
        title: '提示',
        message: '主机离线，无法操作设备。',
        primaryButton: { value: '知道了', action: () => {} }
      });
      return;
    }
    this.confirmLightToggle(!this.lightOn);
  }

  onSmartLightingStart(): void {
    this.confirmSmartLightingStart();
  }

  onDurationSelect(hours: number): void {
    this.selectedDurationKey = `${hours}h`;
    this.customDurationInput = '';
    this.smartDurationMinutes = hours * 60;
  }

  onCustomDurationSelect(): void {
    this.selectedDurationKey = 'custom';
    if (this.customDurationInput.length === 0) {
      this.customDurationInput = '1';
      this.smartDurationMinutes = 60;
      return;
    }
    this.onCustomDurationInputChange(this.customDurationInput);
  }

  onCustomDurationInputChange(value: string): void {
    let digits: string = '';
    for (let i: number = 0; i < value.length; i++) {
      const ch: string = value[i];
      if (ch >= '0' && ch <= '9') {
        digits = `${digits}${ch}`;
      }
    }
    if (digits.length === 0) {
      this.customDurationInput = '';
      return;
    }
    let hours: number = parseInt(digits, 10);
    if (Number.isNaN(hours)) {
      hours = 1;
    }
    if (hours < 1) {
      hours = 1;
    }
    if (hours > 24) {
      hours = 24;
    }
    this.customDurationInput = `${hours}`;
    this.smartDurationMinutes = hours * 60;
    this.selectedDurationKey = 'custom';
  }

  getDurationText(): string {
    return `${Math.floor(this.smartDurationMinutes / 60)}h`;
  }

  confirmLightToggle(targetOn: boolean): void {
    const actionText: string = targetOn ? '开启' : '关闭';
    AlertDialog.show({
      title: '提示',
      message: `确认${actionText}补光设备吗？`,
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestLightToggle(targetOn);
        }
      },
      secondaryButton: {
        value: '取消',
        action: () => {
          this.executeResult = '已取消';
        }
      }
    });
  }

  confirmSmartLightingStart(): void {
    AlertDialog.show({
      title: '提示',
      message: `确认开启智能补光吗？\n控制时长：${this.getDurationText()}`,
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestSmartLightingStart();
        }
      },
      secondaryButton: {
        value: '取消',
        action: () => {
          // 取消操作
        }
      }
    });
  }

  requestLightToggle(targetOn: boolean): void {
    const action: DeviceAction = targetOn ? 'on' : 'off';
    sendDeviceActionCommand('light', action, () => {
      this.clearLightPendingAll();
      this.commandIssuedAt = Date.now();
      this.pendingExpectLightOn = targetOn;
      this.executeResult = targetOn ? '开启指令已发送，等待设备确认…' : '关闭指令已发送，等待设备确认…';
      this.pendingTimerId = setTimeout((): void => {
        this.pendingTimerId = -1;
        const exp: boolean | undefined = this.pendingExpectLightOn;
        if (exp === undefined) {
          return;
        }
        const onNow: boolean = this.lightOn;
        if (exp === true && !onNow) {
          this.executeResult = '开启失败：5秒内未收到开启确认';
          AlertDialog.show({
            title: '提示',
            message: '开启失败：疑似硬件故障或断联。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        } else if (exp === false && onNow) {
          this.executeResult = '关闭失败：5秒内未收到关闭确认';
          AlertDialog.show({
            title: '提示',
            message: '关闭失败：疑似硬件故障或断联。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        }
        this.pendingExpectLightOn = undefined;
      }, PENDING_DEVICE_ACK_MS);
    }, (_msg: string) => {
      this.clearLightPendingAll();
      this.executeResult = targetOn ? '开启指令发送失败' : '关闭指令发送失败';
    });
  }

  requestSmartLightingStart(): void {
    // TODO: 预留一键开启智能补光请求函数
  }

  build() {
    NavDestination() {
      Column({ space: 16 }) {
        Text('智能补光控制')
          .fontSize(22)
          .fontWeight(FontWeight.Bold)
          .fontColor('#333333')

        // 补光设备卡片
        Row({ space: 12 }) {
          Column({ space: 6 }) {
            Text('补光设备')
              .fontSize(18)
              .fontWeight(FontWeight.Medium)
              .fontColor('#333333')
            Text(`当前状态：${this.getLightStatusText()}`)
              .fontSize(13)
              .fontColor(this.lightOn ? '#2E7D32' : '#999999')
            Text(`运行时长：${this.lightRunningMinutes} min`)
              .fontSize(13)
              .fontColor('#666666')
            Text(`设备状态：${this.deviceOnline ? '在线' : '离线'}`)
              .fontSize(13)
              .fontColor(this.deviceOnline ? '#2E7D32' : '#D32F2F')
            if (this.deviceStatusTime) {
              Text(`更新：${this.deviceStatusTime}`)
                .fontSize(12)
                .fontColor('#999999')
            }
            Text(`执行反馈：${this.executeResult}`)
              .fontSize(13)
              .fontColor('#666666')
          }
          .layoutWeight(1)
          .alignItems(HorizontalAlign.Start)

          Button(this.lightOn ? '关闭' : '开启')
            .width(84)
            .height(38)
            .fontSize(14)
            .fontColor(Color.White)
            .backgroundColor('#2E7D32')
            .borderRadius(10)
            .onClick(() => {
              this.onLightButtonClick();
            })
        }
        .width('100%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .alignItems(VerticalAlign.Center)

        // 补光亮度调节（图示组件，硬件未接入）
        Column({ space: 10 }) {
          Row() {
            Text('补光亮度')
              .fontSize(16)
              .fontWeight(FontWeight.Medium)
              .fontColor('#333333')
            Blank()
            Row() {
              Text(`${this.lightBrightness}%`)
                .fontSize(14)
                .fontWeight(FontWeight.Medium)
                .fontColor(Color.White)
            }
            .padding({ left: 12, right: 12, top: 6, bottom: 6 })
            .backgroundColor('#424242')
            .borderRadius(8)
          }
          .width('100%')
          Slider({
            value: this.lightBrightness,
            min: 0,
            max: 100,
            step: 1
          })
            .width('100%')
            .trackColor('#E0E0E0')
            .selectedColor('#42A5F5')
            .trackThickness(4)
            .blockColor(Color.White)
            .trackBorderRadius(2)
            .onChange((value: number) => {
              this.lightBrightness = Math.round(value);
            })
        }
        .width('100%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .alignItems(HorizontalAlign.Start)

        Column({ space: 10 }) {
          Row() {
            Text('控制时长设定')
              .fontSize(16)
              .fontWeight(FontWeight.Medium)
              .fontColor('#333333')
            Blank()
            Row() {
              Text(this.getDurationText())
                .fontSize(14)
                .fontWeight(FontWeight.Medium)
                .fontColor(Color.White)
            }
            .padding({ left: 12, right: 12, top: 6, bottom: 6 })
            .backgroundColor('#424242')
            .borderRadius(8)
          }
          .width('100%')

          Row() {
            Text('1h')
                .fontSize(13)
                .textAlign(TextAlign.Center)
                .fontColor(this.selectedDurationKey === '1h' ? Color.White : '#666666')
                .width('22%')
                .padding({ left: 12, right: 12, top: 6, bottom: 6 })
                .backgroundColor(this.selectedDurationKey === '1h' ? '#2E7D32' : '#F1F3F5')
                .borderRadius(14)
                .onClick(() => {
                  this.onDurationSelect(1);
                })
            Text('2h')
              .fontSize(13)
              .textAlign(TextAlign.Center)
              .fontColor(this.selectedDurationKey === '2h' ? Color.White : '#666666')
              .width('22%')
              .padding({ left: 12, right: 12, top: 6, bottom: 6 })
              .backgroundColor(this.selectedDurationKey === '2h' ? '#2E7D32' : '#F1F3F5')
              .borderRadius(14)
              .onClick(() => {
                this.onDurationSelect(2);
              })
            Text('3h')
              .fontSize(13)
              .textAlign(TextAlign.Center)
              .fontColor(this.selectedDurationKey === '3h' ? Color.White : '#666666')
              .width('22%')
              .padding({ left: 12, right: 12, top: 6, bottom: 6 })
              .backgroundColor(this.selectedDurationKey === '3h' ? '#2E7D32' : '#F1F3F5')
              .borderRadius(14)
              .onClick(() => {
                this.onDurationSelect(3);
              })
            Text('自定义')
              .fontSize(13)
              .textAlign(TextAlign.Center)
              .fontColor(this.selectedDurationKey === 'custom' ? Color.White : '#666666')
              .width('22%')
              .padding({ left: 12, right: 12, top: 6, bottom: 6 })
              .backgroundColor(this.selectedDurationKey === 'custom' ? '#2E7D32' : '#F1F3F5')
              .borderRadius(14)
              .onClick(() => {
                this.onCustomDurationSelect();
              })
          }
          .width('100%')
          .justifyContent(FlexAlign.SpaceBetween)

          if (this.selectedDurationKey === 'custom') {
            Row({ space: 8 }) {
              Text('输入时长')
                .fontSize(13)
                .fontColor('#666666')
              TextInput({ placeholder: '1~24', text: this.customDurationInput })
                .width(86)
                .height(34)
                .fontSize(13)
                .backgroundColor('#F7F7F7')
                .borderRadius(8)
                .onChange((value: string) => {
                  this.onCustomDurationInputChange(value);
                })
              Text('h')
                .fontSize(13)
                .fontColor('#666666')
            }
            .width('100%')
            .alignItems(VerticalAlign.Center)
          }
        }
        .width('100%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .alignItems(HorizontalAlign.Start)

        Button('开启智能补光')
          .width('100%')
          .height(44)
          .fontSize(16)
          .fontColor(Color.White)
          .backgroundColor('#2E7D32')
          .borderRadius(10)
          .onClick(() => {
            this.onSmartLightingStart();
          })
      }
      .width('100%')
      .height('100%')
      .padding(16)
      .alignItems(HorizontalAlign.Start)
      .justifyContent(FlexAlign.Start)
      .backgroundColor('#F0F2F5')
      .expandSafeArea()
    }
    .hideTitleBar(true)
  }
}
