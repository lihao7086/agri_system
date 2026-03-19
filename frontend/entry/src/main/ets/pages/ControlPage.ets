import common from '@ohos.app.ability.common';
import {
  disconnectMqtt as mqttDisconnect,
  getDeviceOnlineStatus,
  getDeviceStatusTime,
  getMqttStatus,
  initMqttConnection as mqttInit,
  registerDeviceStatusListener,
  DeviceStatusListener,
  unregisterDeviceStatusListener
} from './mqtt';

@Component
export struct ControlPage {
  pathStack: NavPathStack = new NavPathStack();
  private readonly onDeviceStatusChanged: DeviceStatusListener = (online: boolean, time?: string): void => {
    this.deviceOnline = online;
    if (time !== undefined) {
      this.deviceStatusTime = time;
    }
  };
  @State deviceConnected: boolean = false;
  @State deviceOnline: boolean = false;
  @State deviceStatusTime: string = '';

  aboutToAppear(): void {
    this.deviceConnected = getMqttStatus();
    this.deviceOnline = getDeviceOnlineStatus();
    this.deviceStatusTime = getDeviceStatusTime();
    registerDeviceStatusListener(this.onDeviceStatusChanged);
  }

  aboutToDisappear(): void {
    unregisterDeviceStatusListener(this.onDeviceStatusChanged);
  }

  build() {
    Column() {
      Row() {
        Text('控制')
          .fontSize(20)
          .fontWeight(FontWeight.Bold)
          .fontColor(Color.White)

        Column({ space: 2 }) {
          Text(this.deviceConnected ? (this.deviceOnline ? '主机在线' : '主机离线') : '未连接')
            .fontSize(14)
            .fontWeight(FontWeight.Medium)
            .fontColor(Color.White)
          if (this.deviceStatusTime) {
            Text(`更新：${this.deviceStatusTime}`)
              .fontSize(11)
              .fontColor('rgba(255,255,255,0.8)')
          }
        }
        .layoutWeight(1)
        .alignItems(HorizontalAlign.Center)

        Button(this.deviceConnected ? '断开连接' : '连接控制中心')
          .height(32)
          .fontSize(12)
          .fontColor('#2E7D32')
          .backgroundColor(Color.White)
          .borderRadius(8)
          .onClick(() => {
            this.handleGatewayAction();
          })
      }
      .width('100%')
      .height(56)
      .padding({ left: 16, right: 16 })
      .backgroundColor('#2E7D32')
      .alignItems(VerticalAlign.Center)

      Scroll() {
        Column({ space: 20 }) {
          // 智能灌溉
          this.ControlSection('智能灌溉', '土壤温湿度采集、灌溉水量计算与控制', 'IrrigationControlPage');
          // 智能补光
          this.ControlSection('智能光照', '光照强度采集、LED补光亮度调节', 'LightingControlPage');
          // 智能通风
          this.ControlSection('智能通风', '通风设备控制 ', 'VentilationControlPage');
          // 智能预警
          this.ControlSection('智能预警', '阈值预警规则配置', 'WarningControlPage');
        }
        .width('100%')
        .padding(16)
        .alignItems(HorizontalAlign.Start)
        .justifyContent(FlexAlign.Start)
      }
      .layoutWeight(1)
      .scrollBar(BarState.Off)
      .align(Alignment.TopStart)
    }
    .width('100%')
    .height('100%')
    .backgroundColor('#F0F2F5')
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
      message: '确认连接控制中心吗？',
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
      message: '确认断开连接吗？',
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
      this.deviceOnline = getDeviceOnlineStatus();
      this.deviceStatusTime = getDeviceStatusTime();
    }, (_msg: string) => {
      this.deviceConnected = false;
      this.deviceOnline = false;
    });
  }

  disconnectMqtt(): void {
    mqttDisconnect();
    this.deviceConnected = false;
    this.deviceOnline = false;
  }

  @Builder
  ControlSection(title: string, desc: string, routeName: string) {
    Column({ space: 12 }) {
      Text(title)
        .fontSize(16)
        .fontWeight(FontWeight.Medium)
        .fontColor('#333333')
        .alignSelf(ItemAlign.Start)
      Text(desc)
        .fontSize(13)
        .fontColor('#666666')
        .alignSelf(ItemAlign.Start)
      Row() {
        Button('进入控制')
          .fontSize(14)
          .fontColor(Color.White)
          .backgroundColor('#2E7D32')
          .borderRadius(8)
          .height(36)
          .onClick(() => {
            this.pathStack.pushPathByName(routeName, null, false);
          })
      }
      .width('100%')
      .justifyContent(FlexAlign.End)
    }
    .width('100%')
    .padding(16)
    .backgroundColor(Color.White)
    .borderRadius(12)
    .alignItems(HorizontalAlign.Start)
  }
}
