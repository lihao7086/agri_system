import hilog from '@ohos.hilog';
import {
  AvgItemModel,
  HTTP_SERVER_CONFIG,
  query7DaysAvgData,
  SevenDaysAvgModel
} from './http';
import {
  registerWeeklyReportListener,
  unregisterWeeklyReportListener,
  WeeklyReportListener,
  WeeklyReportModel
} from './mqtt';

type AvgMetricKey = 'tempAvg' | 'humAvg' | 'co2Avg' | 'phAvg' | 'soilMoistAvg' | 'lightAvg';

interface TrendMetricOption {
  key: AvgMetricKey;
  title: string;
  unit: string;
  color: string;
}

interface ChartPoint {
  x: number;
  y: number;
}

@Builder
export function TrendPageBuilder() {
  TrendPage()
}

@Component
struct TrendPage {
  private readonly chartWidth: number = 332;
  private readonly chartHeight: number = 220;
  private chartReady: boolean = false;
  private chartSettings: RenderingContextSettings = new RenderingContextSettings(true);
  private chartContext: CanvasRenderingContext2D = new CanvasRenderingContext2D(this.chartSettings);

  @State lastQuerySuccess: boolean = false;
  @State loading: boolean = false;
  @State lastUpdateTime: string = '--';
  @State errorMsg: string = '';
  @State deviceId: string = '--';
  @State timeRange: string = '--';
  @State daysCount: number = 0;
  @State avgList: AvgItemModel[] = [];
  @State activeMetricIndex: number = 0;
  @State weeklyReportCode: number = 0;
  @State weeklyReportText: string = '';
  @State weeklyDataPeriod: string = '';
  @State weeklyReportTimestamp: string = '';
  @State weeklyReportDeviceId: string = '';
  private weeklyReportListener: WeeklyReportListener | undefined = undefined;

  private readonly metrics: TrendMetricOption[] = [
    { key: 'tempAvg', title: '温度', unit: '°C', color: '#FF7043' },
    { key: 'humAvg', title: '空气湿度', unit: '%', color: '#42A5F5' },
    { key: 'lightAvg', title: '光照', unit: 'lux', color: '#FFA000' },
    { key: 'co2Avg', title: 'CO₂', unit: 'ppm', color: '#8D6E63' },
    { key: 'soilMoistAvg', title: '土壤湿度', unit: '%', color: '#26A69A' },
    { key: 'phAvg', title: 'PH值', unit: '', color: '#AB47BC' }
  ];

  aboutToAppear(): void {
    this.queryTrendData();
    this.weeklyReportListener = (data: WeeklyReportModel): void => {
      this.weeklyReportCode = data.code;
      this.weeklyReportDeviceId = data.device_id;
      this.weeklyReportText = data.report;
      this.weeklyReportTimestamp = data.timestamp;
      this.weeklyDataPeriod = data.data_period;
    };
    registerWeeklyReportListener(this.weeklyReportListener);
  }

  aboutToDisappear(): void {
    if (this.weeklyReportListener !== undefined) {
      unregisterWeeklyReportListener(this.weeklyReportListener);
      this.weeklyReportListener = undefined;
    }
  }

  private getTodayDateText(): string {
    const now: Date = new Date();
    const year: number = now.getFullYear();
    const month: string = String(now.getMonth() + 1).padStart(2, '0');
    const day: string = String(now.getDate()).padStart(2, '0');
    return `${year}-${month}-${day}`;
  }

  private getItemDateText(rawDate: string): string {
    // 兼容 "YYYY-MM-DD" 或 "YYYY-MM-DD HH:mm:ss"
    return rawDate.length >= 10 ? rawDate.substring(0, 10) : rawDate;
  }

  async queryTrendData(): Promise<void> {
    this.loading = true;
    this.errorMsg = '';
    hilog.info(0x0000, 'TrendPage', `开始查询7天均值, deviceId=${HTTP_SERVER_CONFIG.deviceId}`);
    try {
      const res: SevenDaysAvgModel = await query7DaysAvgData(HTTP_SERVER_CONFIG.deviceId);
      const todayText: string = this.getTodayDateText();
      const filteredList: AvgItemModel[] = res.avgList.filter((item: AvgItemModel): boolean => {
        return this.getItemDateText(item.date) !== todayText;
      });
      this.deviceId = res.deviceId.length > 0 ? res.deviceId : HTTP_SERVER_CONFIG.deviceId;
      this.avgList = filteredList;
      this.daysCount = filteredList.length;
      if (filteredList.length > 0) {
        const startDate: string = this.getItemDateText(filteredList[0].date);
        const endDate: string = this.getItemDateText(filteredList[filteredList.length - 1].date);
        this.timeRange = `${startDate} 至 ${endDate}`;
      } else {
        this.timeRange = '--';
      }
      this.lastUpdateTime = res.timestamp.length > 0 ? res.timestamp : new Date().toLocaleString();
      this.lastQuerySuccess = true;
      hilog.info(0x0000, 'TrendPage',
        `趋势数据过滤完成: 原始${res.avgList.length}条, 过滤当日后${filteredList.length}条, today=${todayText}`);
      this.drawTrendChart();
    } catch (_error) {
      const errorText: string = _error instanceof Error ? _error.message : String(_error);
      this.errorMsg = errorText;
      this.lastQuerySuccess = false;
      this.lastUpdateTime = '--';
      this.avgList = [];
      hilog.error(0x0000, 'TrendPage', `7天均值查询失败: ${errorText}`);
      this.drawTrendChart();
    } finally {
      this.loading = false;
    }
  }

  getCurrentMetric(): TrendMetricOption {
    return this.metrics[this.activeMetricIndex];
  }

  getMetricValue(item: AvgItemModel, key: AvgMetricKey): number {
    if (key === 'tempAvg') {
      return item.tempAvg;
    }
    if (key === 'humAvg') {
      return item.humAvg;
    }
    if (key === 'co2Avg') {
      return item.co2Avg;
    }
    if (key === 'phAvg') {
      return item.phAvg;
    }
    if (key === 'soilMoistAvg') {
      return item.soilMoistAvg;
    }
    return item.lightAvg;
  }

  getMetricValueText(value: number): string {
    return Number.isInteger(value) ? `${value}` : `${value.toFixed(1)}`;
  }

  private drawTrendChart(): void {
    if (!this.chartReady) {
      return;
    }

    const canvasW: number = this.chartWidth;
    const canvasH: number = this.chartHeight;
    const ctx: CanvasRenderingContext2D = this.chartContext;
    ctx.clearRect(0, 0, canvasW, canvasH);
    if (this.avgList.length === 0) {
      return;
    }

    const metric: TrendMetricOption = this.getCurrentMetric();
    const values: number[] = this.avgList.map((item: AvgItemModel): number => this.getMetricValue(item, metric.key));
    let minVal: number = values[0];
    let maxVal: number = values[0];
    for (let i: number = 1; i < values.length; i++) {
      if (values[i] < minVal) {
        minVal = values[i];
      }
      if (values[i] > maxVal) {
        maxVal = values[i];
      }
    }
    if (maxVal === minVal) {
      maxVal = maxVal + 1;
      minVal = minVal - 1;
    }

    const leftPad: number = 34;
    const rightPad: number = 14;
    const topPad: number = 30;
    const bottomPad: number = 36;
    const plotW: number = canvasW - leftPad - rightPad;
    const plotH: number = canvasH - topPad - bottomPad;

    ctx.strokeStyle = '#E0E0E0';
    ctx.lineWidth = 1;
    for (let i: number = 0; i < 4; i++) {
      const y: number = topPad + (plotH * i) / 3;
      ctx.beginPath();
      ctx.moveTo(leftPad, y);
      ctx.lineTo(leftPad + plotW, y);
      ctx.stroke();
    }

    const points: ChartPoint[] = [];
    const xStep: number = this.avgList.length > 1 ? plotW / (this.avgList.length - 1) : plotW;
    for (let i: number = 0; i < this.avgList.length; i++) {
      const ratio: number = (values[i] - minVal) / (maxVal - minVal);
      const point: ChartPoint = {
        x: leftPad + xStep * i,
        y: topPad + plotH - ratio * plotH
      };
      points.push(point);
    }

    ctx.strokeStyle = metric.color;
    ctx.lineWidth = 2;
    ctx.beginPath();
    ctx.moveTo(points[0].x, points[0].y);
    for (let i: number = 1; i < points.length; i++) {
      ctx.lineTo(points[i].x, points[i].y);
    }
    ctx.stroke();

    ctx.fillStyle = metric.color;
    for (let i: number = 0; i < points.length; i++) {
      ctx.beginPath();
      ctx.arc(points[i].x, points[i].y, 3, 0, Math.PI * 2);
      ctx.fill();
    }

    ctx.fillStyle = '#424242';
    ctx.font = '34px sans-serif';
    for (let i: number = 0; i < points.length; i++) {
      const valueText: string = this.getMetricValueText(values[i]);
      const textX: number = points[i].x - valueText.length * 3;
      const textY: number = Math.max(topPad + 12, points[i].y - 10);
      ctx.fillText(valueText, textX, textY);
    }

    const xLabelY: number = topPad + plotH + 16;
    ctx.fillStyle = '#757575';
    ctx.font = '34px sans-serif';
    for (let i: number = 0; i < this.avgList.length; i++) {
      const dateText: string = this.avgList[i].date.length >= 5 ? this.avgList[i].date.substring(5) : this.avgList[i].date;
      ctx.fillText(dateText, points[i].x - 12, xLabelY);
    }
  }

  build() {
    NavDestination() {
      Column({ space: 12 }) {
        Row() {
          Text('趋势分析')
            .fontSize(20)
            .fontWeight(FontWeight.Bold)
            .fontColor(Color.White)

          Text(this.lastQuerySuccess ? '最近一次查询成功' : '最近一次查询失败')
            .layoutWeight(1)
            .textAlign(TextAlign.Center)
            .fontSize(14)
            .fontWeight(FontWeight.Medium)
            .fontColor(Color.White)

          Image($r('app.media.refresh'))
            .width(20)
            .height(20)
            .fillColor(Color.White)
            .onClick(() => {
              this.queryTrendData();
            })
        }
        .width('100%')
        .height(56)
        .padding({ left: 16, right: 16 })
        .backgroundColor('#2E7D32')
        .alignItems(VerticalAlign.Center)

        Scroll() {
          Column({ space: 12 }) {
            Column({ space: 10 }) {
              Text(`设备ID：${this.deviceId}`)
                .fontSize(14)
                .fontWeight(FontWeight.Medium)
                .fontColor('#333333')
                .alignSelf(ItemAlign.Start)
              Text(`统计范围：${this.timeRange}`)
                .fontSize(13)
                .fontColor('#666666')
                .alignSelf(ItemAlign.Start)
              Text(`天数：${this.daysCount}`)
                .fontSize(13)
                .fontColor('#666666')
                .alignSelf(ItemAlign.Start)
              Text(`最后更新时间：${this.lastUpdateTime}`)
                .fontSize(12)
                .fontColor('#999999')
                .alignSelf(ItemAlign.Start)
            }
            .width('100%')
            .padding(14)
            .backgroundColor(Color.White)
            .borderRadius(12)
            .alignItems(HorizontalAlign.Start)

            if (this.loading) {
              Column() {
                Text('正在查询近7天平均值数据...')
                  .fontSize(13)
                  .fontColor('#666666')
              }
              .width('100%')
              .constraintSize({ minHeight: 280 })
              .justifyContent(FlexAlign.Center)
              .alignItems(HorizontalAlign.Center)
            } else if (!this.lastQuerySuccess) {
              Column() {
                Text(`查询失败：${this.errorMsg.length > 0 ? this.errorMsg : '未知错误'}`)
                  .fontSize(13)
                  .fontColor(Color.Red)
                  .textAlign(TextAlign.Center)
              }
              .width('100%')
              .constraintSize({ minHeight: 280 })
              .justifyContent(FlexAlign.Center)
              .alignItems(HorizontalAlign.Center)
            } else if (this.avgList.length === 0) {
              Column() {
                Text('暂无近7天平均值数据')
                  .fontSize(13)
                  .fontColor('#666666')
              }
              .width('100%')
              .constraintSize({ minHeight: 280 })
              .justifyContent(FlexAlign.Center)
              .alignItems(HorizontalAlign.Center)
            } else {
              Column({ space: 10 }) {
                Swiper() {
                  ForEach(this.metrics, (metric: TrendMetricOption, index: number) => {
                    Column({ space: 6 }) {
                      Text(`近7天${metric.title}每日平均值`)
                        .fontSize(15)
                        .fontWeight(FontWeight.Medium)
                        .fontColor('#333333')
                      Text(`单位：${metric.unit.length > 0 ? metric.unit : '--'}`)
                        .fontSize(12)
                        .fontColor('#666666')
                    }
                    .width('100%')
                    .height('100%')
                    .justifyContent(FlexAlign.Center)
                    .alignItems(HorizontalAlign.Center)
                  }, (metric: TrendMetricOption, index: number) => `${metric.key}-${index}`)
                }
                .width('100%')
                .height(74)
                .autoPlay(true)
                .interval(5000)
                .loop(true)
                .indicator(false)
                .onChange((index: number) => {
                  this.activeMetricIndex = index;
                  this.drawTrendChart();
                })

                Canvas(this.chartContext)
                  .width(this.chartWidth)
                  .height(this.chartHeight)
                  .backgroundColor(Color.White)
                  .onReady(() => {
                    this.chartReady = true;
                    this.drawTrendChart();
                  })

                Row({ space: 6 }) {
                  ForEach(this.metrics, (_metric: TrendMetricOption, index: number) => {
                    Circle()
                      .width(6)
                      .height(6)
                      .fill(index === this.activeMetricIndex ? '#2E7D32' : '#D0D0D0')
                  }, (_metric: TrendMetricOption, index: number) => `${index}`)
                }
                .width('100%')
                .justifyContent(FlexAlign.Center)
                .padding({ top: 2, bottom: 2 })
              }
              .width('100%')
              .padding(12)
              .backgroundColor(Color.White)
              .borderRadius(12)
              .alignItems(HorizontalAlign.Center)
              .justifyContent(FlexAlign.Start)

              Column({ space: 10 }) {
                Row({ space: 8 }) {
                  Image($r('app.media.ai'))
                    .width(24)
                    .height(24)
                    .fillColor('#2E7D32')
                  Text('周报分析与未来预测')
                    .fontSize(16)
                    .fontWeight(FontWeight.Medium)
                    .fontColor('#333333')
                }
                .width('100%')
                .alignItems(VerticalAlign.Center)

                if (this.weeklyDataPeriod.length > 0) {
                  Text(`数据周期：${this.weeklyDataPeriod}`)
                    .fontSize(13)
                    .fontColor('#666666')
                    .width('100%')
                }
                if (this.weeklyReportTimestamp.length > 0) {
                  Text(`生成时间：${this.weeklyReportTimestamp}`)
                    .fontSize(12)
                    .fontColor('#999999')
                    .width('100%')
                }
                if (this.weeklyReportDeviceId.length > 0) {
                  Text(`设备：${this.weeklyReportDeviceId}`)
                    .fontSize(12)
                    .fontColor('#999999')
                    .width('100%')
                }

                if (this.weeklyReportText.length > 0) {
                  Text(this.weeklyReportText)
                    .fontSize(14)
                    .fontColor('#333333')
                    .lineHeight(22)
                    .width('100%')
                } else {
                  Text('暂无周报内容，请等待服务端推送至主题 agri/server/weekly_report(Markdown 正文将显示于此）')
                    .fontSize(13)
                    .fontColor('#999999')
                    .lineHeight(20)
                    .width('100%')
                }

                if (this.weeklyReportCode !== 0 && this.weeklyReportCode !== 200) {
                  Text(`状态码：${this.weeklyReportCode}`)
                    .fontSize(12)
                    .fontColor('#C62828')
                    .width('100%')
                }
              }
              .width('100%')
              .padding(14)
              .backgroundColor(Color.White)
              .borderRadius(12)
              .alignItems(HorizontalAlign.Start)
            }
          }
          .width('100%')
          .padding({ left: 20, right: 20, top: 4, bottom: 16 })
          .alignItems(HorizontalAlign.Center)
        }
        .width('100%')
        .layoutWeight(1)
        .scrollBar(BarState.Auto)
        .edgeEffect(EdgeEffect.Spring)
        .align(Alignment.Top)
      }
      .width('100%')
      .height('100%')
      .backgroundColor('#F0F2F5')
      .alignItems(HorizontalAlign.Start)
      .expandSafeArea()
    }
    .hideTitleBar(true)
  }
}
