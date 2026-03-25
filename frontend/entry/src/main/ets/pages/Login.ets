@Builder
export function LoginBuilder(){
  Login()
}

@Component
struct Login {
  pathStack: NavPathStack = new NavPathStack();
  @State account: string = ''
  @State password: string = ''

  build() {
    NavDestination(){
      Column() {
        Column() {
          Text('智慧农业')
            .fontSize(28)
            .fontWeight(FontWeight.Bold)
            .fontColor('#2E7D32')
            .margin({ top: 60, bottom: 8 })

          Text('远程监测 · 智能控制')
            .fontSize(14)
            .fontColor('#666666')
            .margin({ bottom: 48 })

          Column({ space: 6 }) {
            Text('账号')
              .fontSize(14)
              .fontColor('#333333')
              .alignSelf(ItemAlign.Start)
            TextInput({ placeholder: '请输入账号', text: this.account })
              .type(InputType.Normal)
              .height(48)
              .backgroundColor('#F5F5F5')
              .borderRadius(8)
              .padding({ left: 16, right: 16 })
              .onChange((value: string) => {
                this.account = value;
              })

          }
          .width('100%')
          .alignItems(HorizontalAlign.Start)
          .margin({ bottom: 20 })

          Column({ space: 6 }) {
            Text('密码')
              .fontSize(14)
              .fontColor('#333333')
              .alignSelf(ItemAlign.Start)
            TextInput({ placeholder: '请输入密码', text: this.password })
              .type(InputType.Password)
              .height(48)
              .backgroundColor('#F5F5F5')
              .borderRadius(8)
              .padding({ left: 16, right: 16 })
              .onChange((value: string) => {
                this.password = value;
              })

          }
          .width('100%')
          .alignItems(HorizontalAlign.Start)
          .margin({ bottom: 32 })

          Button('登录')
            .width('100%')
            .height(48)
            .fontSize(16)
            .fontWeight(FontWeight.Medium)
            .backgroundColor('#2E7D32')
            .borderRadius(8)
            .onClick(() => {
                this.pathStack.replacePathByName("MainLayout",{},false)
            })
            .margin({ bottom: 24 })

          Button('注册')
            .width('100%')
            .height(48)
            .fontSize(16)
            .fontColor('#2E7D32')
            .backgroundColor(Color.White)
            .border({ width: 1, color: '#2E7D32' })
            .borderRadius(8)
            //.onClick(() => {
              // 注册逻辑预留
            //})
        }
        .width('85%')
        .padding({ left: 24, right: 24 })
    }
    .width('100%')
    .height('100%')
    .backgroundColor(Color.White)
    .expandSafeArea()
   }
    .onReady((context: NavDestinationContext)=>{
      this.pathStack=context.pathStack
    })
  }
      
}
