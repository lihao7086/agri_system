import { HomePage } from './HomePage';
import { ControlPage } from './ControlPage';
import { ProfilePage } from './ProfilePage';

@Builder
export function MainLayoutBuilder(){
  MainLayout()
}

interface TabItem {
  title: string;
  icon: ResourceStr;
}



@ComponentV2
struct MainLayout {
  pathStack: NavPathStack = new NavPathStack();
  @Local currentIndex: number = 0;
  tabItems: TabItem[] = [
    { title: '首页', icon: $r("app.media.home") },
    { title: '控制', icon: $r("app.media.control") },
    { title: '我的', icon: $r("app.media.profile") },
  ];

  @Builder
  TabBuilder(item: TabItem, index: number) {
    Column() {
      Image(item.icon)
        .width(24)
        .height(24)
        .margin({ bottom: 4 })
        .fillColor(this.currentIndex === index ? '#2E7D32' : '#999999')
      Text(item.title)
        .fontSize(this.currentIndex === index ? 14 : 13)
        .fontColor(this.currentIndex === index ? '#2E7D32' : '#999999')
        .fontWeight(this.currentIndex === index ? FontWeight.Medium : FontWeight.Normal)
    }
    .width('100%')
    .height('100%')
    .justifyContent(FlexAlign.Center)
    .onClick(() => {
      this.currentIndex = index;
    })
  }

  build() {
    NavDestination(){
      Tabs({ barPosition: BarPosition.End, index: this.currentIndex }) {
        ForEach(this.tabItems, (item: TabItem, index: number) => {
          TabContent() {
            if(this.currentIndex === 0){
              HomePage({ pathStack: this.pathStack })
            }else if(this.currentIndex === 1){
              ControlPage({ pathStack: this.pathStack })
            }else if(this.currentIndex === 2){
              ProfilePage({ pathStack: this.pathStack })
            }
          }
            .tabBar(this.TabBuilder(this.tabItems[index], index))
            .expandSafeArea([SafeAreaType.SYSTEM],[SafeAreaEdge.TOP,SafeAreaEdge.BOTTOM])
        })
      }
        .width('100%')
        .height('100%')
        .barHeight(56)
        .barBackgroundColor(Color.White)
        .animationDuration(0)
        .onChange((index: number) => {
          this.currentIndex = index;
        })
        .expandSafeArea([SafeAreaType.SYSTEM],[SafeAreaEdge.TOP,SafeAreaEdge.BOTTOM])
    }
      .onReady((context: NavDestinationContext)=>{
        this.pathStack=context.pathStack
      })   
  }
}
