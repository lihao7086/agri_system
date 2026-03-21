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
/** 忽略指令发出后极短时间内的旧包，避免误判 */
const MIN_MS_FOR_OPPOSITE_ACK: number = 200;

@Builder
export function IrrigationControlPageBuilder() {
  IrrigationControlPage()
}

@Component
struct IrrigationControlPage {
  private pendingExpectValveOn: boolean | undefined = undefined;
  private pendingTimerId: number = -1;
  private commandIssuedAt: number = 0;

  private clearValvePendingTimer(): void {
    if (this.pendingTimerId !== -1) {
      clearTimeout(this.pendingTimerId);
      this.pendingTimerId = -1;
    }
  }

  private clearValvePendingAll(): void {
    this.clearValvePendingTimer();
    this.pendingExpectValveOn = undefined;
  }

  private readonly onControlStateChanged: DeviceControlStateListener = (state: ControlStatusModel): void => {
    this.valveOn = state.valveState;
    if (this.pendingExpectValveOn === true && state.valveState) {
      this.clearValvePendingAll();
      this.executeResult = '开启成功（设备已上报运行）';
    } else if (this.pendingExpectValveOn === false && !state.valveState) {
      this.clearValvePendingAll();
      this.executeResult = '关闭成功（设备已上报关闭）';
    }
  };

  private readonly onSwitchReport: DeviceSwitchReportListener = (r: DeviceSwitchReportModel): void => {
    if (r.device_type !== 'valve') {
      return;
    }
    if (Date.now() - this.commandIssuedAt < MIN_MS_FOR_OPPOSITE_ACK) {
      return;
    }
    if (this.pendingExpectValveOn === true && r.status === 'off') {
      this.clearValvePendingAll();
      this.executeResult = '开启失败：硬件反馈为关闭状态';
      AlertDialog.show({
        title: '提示',
        message: '开启失败：设备上报仍为关闭状态。',
        primaryButton: { value: '知道了', action: () => {} }
      });
    } else if (this.pendingExpectValveOn === false && r.status === 'on') {
      this.clearValvePendingAll();
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
  @State valveOn: boolean = false;
  @State valveRunningMinutes: number = 0;
  @State deviceConnected: boolean = false;
  @State deviceOnline: boolean = false;
  @State deviceStatusTime: string = '';
  @State executeResult: string = '未执行';

  aboutToAppear(): void {
    const controlState: ControlStatusModel = getDeviceControlState();
    this.valveOn = controlState.valveState;
    this.deviceConnected = getMqttStatus();
    this.deviceOnline = getDeviceOnlineStatus();
    this.deviceStatusTime = getDeviceStatusTime();
    registerDeviceControlStateListener(this.onControlStateChanged);
    registerDeviceStatusListener(this.onDeviceStatusChanged);
    registerDeviceSwitchReportListener(this.onSwitchReport);
  }

  aboutToDisappear(): void {
    this.clearValvePendingAll();
    unregisterDeviceControlStateListener(this.onControlStateChanged);
    unregisterDeviceStatusListener(this.onDeviceStatusChanged);
    unregisterDeviceSwitchReportListener(this.onSwitchReport);
  }

  getValveStatusText(): string {
    return this.valveOn ? '运行中' : '已关闭';
  }

  onValveButtonClick(): void {
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
    this.confirmValveToggle(!this.valveOn);
  }

  onSmartIrrigationStart(): void {
    this.confirmSmartIrrigationStart();
  }

  confirmValveToggle(targetOn: boolean): void {
    const actionText: string = targetOn ? '开启' : '关闭';
    AlertDialog.show({
      title: '提示',
      message: `确认${actionText}水阀设备吗？`,
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestValveToggle(targetOn);
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

  confirmSmartIrrigationStart(): void {
    AlertDialog.show({
      title: '提示',
      message: '确认开启智能灌溉吗？',
      primaryButton: {
        value: '确认',
        action: () => {
          this.requestSmartIrrigationStart();
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

  requestValveToggle(targetOn: boolean): void {
    const action: DeviceAction = targetOn ? 'on' : 'off';
    sendDeviceActionCommand('valve', action, () => {
      this.clearValvePendingAll();
      this.commandIssuedAt = Date.now();
      this.pendingExpectValveOn = targetOn;
      this.executeResult = targetOn ? '开启指令已发送，等待设备确认…' : '关闭指令已发送，等待设备确认…';
      this.pendingTimerId = setTimeout((): void => {
        this.pendingTimerId = -1;
        const exp: boolean | undefined = this.pendingExpectValveOn;
        if (exp === undefined) {
          return;
        }
        if (exp === true && !this.valveOn) {
          this.executeResult = '开启失败：5秒内未收到开启确认';
          AlertDialog.show({
            title: '提示',
            message: '开启失败：设备未在5秒内反馈为开启状态，或硬件仍为关闭。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        } else if (exp === false && this.valveOn) {
          this.executeResult = '关闭失败：5秒内未收到关闭确认';
          AlertDialog.show({
            title: '提示',
            message: '关闭失败：设备未在5秒内反馈为关闭状态，或硬件仍在运行。',
            primaryButton: { value: '知道了', action: () => {} }
          });
        }
        this.pendingExpectValveOn = undefined;
      }, PENDING_DEVICE_ACK_MS);
    }, (_msg: string) => {
      this.clearValvePendingAll();
      this.executeResult = targetOn ? '开启指令发送失败' : '关闭指令发送失败';
    });
  }

  requestSmartIrrigationStart(): void {
    // TODO: 预留一键开启智能灌溉请求函数
  }

  build() {
    NavDestination() {
      Column({ space: 16 }) {
        Text('智能灌溉控制')
          .fontSize(22)
          .fontWeight(FontWeight.Bold)
          .fontColor('#333333')

        // 水阀设备卡片
        Row({ space: 12 }) {
          Column({ space: 6 }) {
            Text('水阀设备')
              .fontSize(18)
              .fontWeight(FontWeight.Medium)
              .fontColor('#333333')
            Text(`当前状态：${this.getValveStatusText()}`)
              .fontSize(13)
              .fontColor(this.valveOn ? '#2E7D32' : '#999999')
            Text(`运行时长：${this.valveRunningMinutes} min`)
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

          Button(this.valveOn ? '关闭' : '开启')
            .width(84)
            .height(38)
            .fontSize(14)
            .fontColor(Color.White)
            .backgroundColor('#2E7D32')
            .borderRadius(10)
            .onClick(() => {
              this.onValveButtonClick();
            })
        }
        .width('100%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .alignItems(VerticalAlign.Center)

        Button('开启智能灌溉')
          .width('100%')
          .height(44)
          .fontSize(16)
          .fontColor(Color.White)
          .backgroundColor('#2E7D32')
          .borderRadius(10)
          .onClick(() => {
            this.onSmartIrrigationStart();
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
