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
export function VentilationControlPageBuilder() {
  VentilationControlPage()
}

@Component
struct VentilationControlPage {
  private pendingExpectPumpOn: boolean | undefined = undefined;
  private pendingTimerId: number = -1;
  private commandIssuedAt: number = 0;

  private clearPumpPendingTimer(): void {
    if (this.pendingTimerId !== -1) {
      clearTimeout(this.pendingTimerId);
      this.pendingTimerId = -1;
    }
  }

  private clearPumpPendingAll(): void {
    this.clearPumpPendingTimer();
    this.pendingExpectPumpOn = undefined;
  }

  private readonly onControlStateChanged: DeviceControlStateListener = (state: ControlStatusModel): void => {
    this.fanOn = state.pumpSpeed > 0;
    if (this.pendingExpectPumpOn === true && state.pumpSpeed > 0) {
      this.clearPumpPendingAll();
      this.executeResult = '开启成功（设备已上报运行）';
    } else if (this.pendingExpectPumpOn === false && state.pumpSpeed <= 0) {
      this.clearPumpPendingAll();
      this.executeResult = '关闭成功（设备已上报关闭）';
    }
  };

  private readonly onSwitchReport: DeviceSwitchReportListener = (r: DeviceSwitchReportModel): void => {
    if (r.device_type !== 'pump') {
      return;
    }
    if (Date.now() - this.commandIssuedAt < MIN_MS_FOR_OPPOSITE_ACK) {
      return;
    }
    if (this.pendingExpectPumpOn === true && r.status === 'off') {
      this.clearPumpPendingAll();
      this.executeResult = '开启失败：硬件反馈为关闭状态';
      AlertDialog.show({
        title: '提示',
        message: '开启失败：设备上报仍为关闭状态。',
        primaryButton: { value: '知道了', action: () => {} }
      });
    } else if (this.pendingExpectPumpOn === false && r.status === 'on') {
      this.clearPumpPendingAll();
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
  @State fanOn: boolean = false;
  @State fanRunningMinutes: number = 0;
  @State deviceConnected: boolean = false;
  @State deviceOnline: boolean = false;
  @State deviceStatusTime: string = '';
  @State executeResult: string = '未执行';

  aboutToAppear(): void {
    const controlState: ControlStatusModel = getDeviceControlState();
    this.fanOn = controlState.pumpSpeed > 0;
    this.deviceConnected = getMqttStatus();
    this.deviceOnline = getDeviceOnlineStatus();
    this.deviceStatusTime = getDeviceStatusTime();
    registerDeviceControlStateListener(this.onControlStateChanged);
    registerDeviceStatusListener(this.onDeviceStatusChanged);
    registerDeviceSwitchReportListener(this.onSwitchReport);
  }

  aboutToDisappear(): void {
    this.clearPumpPendingAll();
    unregisterDeviceControlStateListener(this.onControlStateChanged);
    unregisterDeviceStatusListener(this.onDeviceStatusChanged);
    unregisterDeviceSwitchReportListener(this.onSwitchReport);
  }

  getFanStatusText(): string {
    return this.fanOn ? '运行中' : '已关闭';
  }

  onFanButtonClick(): void {
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
    this.confirmFanToggle(!this.fanOn);
  }

  onSmartVentilationStart(): void {
    this.confirmSmartVentilationStart();
  }

  confirmFanToggle(targetOn: boolean): void {
    const actionText: string = targetOn ? '开启' : '关闭';
    AlertDialog.show({
      title: '提示',
      message: `确认${actionText}通风设备吗？`,
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestFanToggle(targetOn);
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

  confirmSmartVentilationStart(): void {
    AlertDialog.show({
      title: '提示',
      message: '确认开启智能通风吗？',
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestSmartVentilationStart();
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

  requestFanToggle(targetOn: boolean): void {
    const action: DeviceAction = targetOn ? 'on' : 'off';
    sendDeviceActionCommand('pump', action, () => {
      this.clearPumpPendingAll();
      this.commandIssuedAt = Date.now();
      this.pendingExpectPumpOn = targetOn;
      this.executeResult = targetOn ? '开启指令已发送，等待设备确认…' : '关闭指令已发送，等待设备确认…';
      this.pendingTimerId = setTimeout((): void => {
        this.pendingTimerId = -1;
        const exp: boolean | undefined = this.pendingExpectPumpOn;
        if (exp === undefined) {
          return;
        }
        if (exp === true && !this.fanOn) {
          this.executeResult = '开启失败：5秒内未收到开启确认';
          AlertDialog.show({
            title: '提示',
            message: '开启失败：设备未在5秒内反馈为开启状态，或硬件仍为关闭。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        } else if (exp === false && this.fanOn) {
          this.executeResult = '关闭失败：5秒内未收到关闭确认';
          AlertDialog.show({
            title: '提示',
            message: '关闭失败：设备未在5秒内反馈为关闭状态，或硬件仍在运行。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        }
        this.pendingExpectPumpOn = undefined;
      }, PENDING_DEVICE_ACK_MS);
    }, (_msg: string) => {
      this.clearPumpPendingAll();
      this.executeResult = targetOn ? '开启指令发送失败' : '关闭指令发送失败';
    });
  }

  requestSmartVentilationStart(): void {
    // TODO: 预留一键开启智能通风请求函数
  }

  build() {
    NavDestination() {
      Column({ space: 16 }) {
        Text('智能通风控制')
          .fontSize(22)
          .fontWeight(FontWeight.Bold)
          .fontColor('#333333')

        // 通风设备卡片
        Row({ space: 12 }) {
          Column({ space: 6 }) {
            Text('通风设备')
              .fontSize(18)
              .fontWeight(FontWeight.Medium)
              .fontColor('#333333')
            Text(`当前状态：${this.getFanStatusText()}`)
              .fontSize(13)
              .fontColor(this.fanOn ? '#2E7D32' : '#999999')
            Text(`运行时长：${this.fanRunningMinutes} min`)
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

          Button(this.fanOn ? '关闭' : '开启')
            .width(84)
            .height(38)
            .fontSize(14)
            .fontColor(Color.White)
            .backgroundColor('#2E7D32')
            .borderRadius(10)
            .onClick(() => {
              this.onFanButtonClick();
            })
        }
        .width('100%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .alignItems(VerticalAlign.Center)

        Button('开启智能通风')
          .width('100%')
          .height(44)
          .fontSize(16)
          .fontColor(Color.White)
          .backgroundColor('#2E7D32')
          .borderRadius(10)
          .onClick(() => {
            this.onSmartVentilationStart();
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
