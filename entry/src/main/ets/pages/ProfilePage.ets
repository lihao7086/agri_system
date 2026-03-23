@Component
export struct ProfilePage {
  pathStack: NavPathStack = new NavPathStack();
  @State showFaultDialog: boolean = false;

  build() {
    Stack() {
      Column() {
      Row() {
        Text('我的')
          .fontSize(20)
          .fontWeight(FontWeight.Bold)
          .fontColor(Color.White)

        Blank()

        Image($r('app.media.chat'))
          .width(28)
          .height(28)
          .onClick(() => {
            this.pathStack.pushPathByName('ChatPage', null, false);
          })
      }
      .width('100%')
      .height(56)
      .padding({ left: 16, right: 25 })
      .backgroundColor('#2E7D32')
      .alignItems(VerticalAlign.Center)

      Scroll() {
        Column({ space: 12 }) {
          // 用户信息区
          Row({ space: 16 }) {
            Image($r('app.media.bird'))
              .width(56)
              .height(56)
              .borderRadius(28)
              .objectFit(ImageFit.Cover)
            Column({ space: 4 }) {
              Text('test1')
                .fontSize(18)
                .fontWeight(FontWeight.Medium)
                .fontColor('#333333')
              Text('智慧农业账号')
                .fontSize(13)
                .fontColor('#666666')
            }
            .alignItems(HorizontalAlign.Start)
            .alignSelf(ItemAlign.Center)
          }
          .width('100%')
          .padding(20)
          .backgroundColor(Color.White)
          .borderRadius(12)

          // 菜单列表
          this.MenuItem('AI农技问答', '问题诊断与建议', () => {
            this.pathStack.pushPathByName('AiQaPage', null, false);
          })
          this.MenuItem('关于', '版本与说明', () => {
            this.showDeviceFaultAlert();
          });
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

      if (this.showFaultDialog) {
        Column() {}
          .width('100%')
          .height('100%')
          .backgroundColor('#66000000')
          .onClick(() => {
            this.showFaultDialog = false;
          })

        Column({ space: 14 }) {
          Text('设备故障警报')
            .fontSize(18)
            .fontWeight(FontWeight.Medium)
            .fontColor('#333333')

          Row({ space: 10 }) {
            Image($r('app.media.warning'))
              .width(22)
              .height(22)
            Text('时间：03-18 14:00\n地点：大棚1\n检测到风扇故障')
              .fontSize(14)
              .lineHeight(22)
              .fontColor('#333333')
          }
          .width('100%')
          .alignItems(VerticalAlign.Top)

          Row({ space: 10 }) {
            Button('关闭')
              .layoutWeight(1)
              .height(40)
              .fontSize(15)
              .fontColor('#333333')
              .backgroundColor('#EEF1F4')
              .borderRadius(8)
              .onClick(() => {
                this.showFaultDialog = false;
              })

            Button('转发至大棚1')
              .layoutWeight(1)
              .height(40)
              .fontSize(15)
              .fontColor(Color.White)
              .backgroundColor('#2E7D32')
              .borderRadius(8)
              .onClick(() => {
                this.showFaultDialog = false;
                this.showForwardSuccessAlert();
              })
          }
          .width('100%')
        }
        .width('86%')
        .padding(16)
        .backgroundColor(Color.White)
        .borderRadius(12)
        .align(Alignment.Center)
        .onClick(() => {
          // 阻止点击弹窗内容时触发蒙层关闭
        })
      }
    }
    .width('100%')
    .height('100%')
  }

  @Builder
  MenuItem(title: string, subtitle: string, onPress?: () => void) {
    Row() {
      Column({ space: 2 }) {
        Text(title)
          .fontSize(15)
          .fontColor('#333333')
        Text(subtitle)
          .fontSize(12)
          .fontColor('#999999')
      }
      .alignItems(HorizontalAlign.Start)
      .layoutWeight(1)
    }
    .width('100%')
    .height(56)
    .padding({ left: 16, right: 16 })
    .backgroundColor(Color.White)
    .borderRadius(12)
    .onClick(() => {
      if (onPress) {
        onPress();
      }
    })
  }

  private showDeviceFaultAlert(): void {
    this.showFaultDialog = true;
  }

  private showForwardSuccessAlert(): void {
    AlertDialog.show({
      title: '转发结果',
      message: '故障信息已转发到大棚1（4）',
      primaryButton: {
        value: '知道了',
        action: () => {}
      }
    });
  }
}
