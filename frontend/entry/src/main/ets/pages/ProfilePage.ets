@Component
export struct ProfilePage {
  pathStack: NavPathStack = new NavPathStack();

  build() {
    Column() {
      Row() {
        Text('我的')
          .fontSize(20)
          .fontWeight(FontWeight.Bold)
          .fontColor(Color.White)

        Blank()

      }
      .width('100%')
      .height(56)
      .padding({ left: 16, right: 16 })
      .backgroundColor('#2E7D32')
      .alignItems(VerticalAlign.Center)

      Scroll() {
        Column({ space: 12 }) {
          // 用户信息区
          Row({ space: 16 }) {
            Column()
              .width(56)
              .height(56)
              .borderRadius(28)
              .backgroundColor('#E8F5E9')
            Column({ space: 4 }) {
              Text('用户')
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
          this.MenuItem('设备管理', '网关与设备连接');
          this.MenuItem('预警设置', '阈值与预警规则');
          this.MenuItem('关于', '版本与说明');
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

  @Builder
  MenuItem(title: string, subtitle: string) {
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
      .onClick(() => {})
  }
}
