import common from '@ohos.app.ability.common';
import preferences from '@ohos.data.preferences';
import promptAction from '@ohos.promptAction';
import {
  ThresholdConfigItem,
  ThresholdConfigModel,
  ThresholdMetric,
  getThresholdConfig,
  updateThresholdConfig
} from './mqtt';

interface ThresholdRowViewModel {
  metric: ThresholdMetric;
  title: string;
  unit: string;
  valueMinLimit: number;
  valueMaxLimit: number;
  valueStep: number;
  hysteresisMax: number;
  hysteresisStep: number;
}

const THRESHOLD_PREF_NAME: string = 'agri_thresholds';
const THRESHOLD_PREF_KEY: string = 'warning_thresholds';

const THRESHOLD_ROWS: ThresholdRowViewModel[] = [
  { metric: 'temp', title: '空气温度', unit: '°C', valueMinLimit: -10, valueMaxLimit: 50, valueStep: 0.5, hysteresisMax: 5, hysteresisStep: 0.1 },
  { metric: 'airHum', title: '空气湿度', unit: '%', valueMinLimit: 0, valueMaxLimit: 100, valueStep: 1, hysteresisMax: 20, hysteresisStep: 0.5 },
  { metric: 'light', title: '光照强度', unit: 'lux', valueMinLimit: 0, valueMaxLimit: 20000, valueStep: 10, hysteresisMax: 3000, hysteresisStep: 10 },
  { metric: 'co2', title: 'CO₂ 浓度', unit: 'ppm', valueMinLimit: 300, valueMaxLimit: 5000, valueStep: 10, hysteresisMax: 800, hysteresisStep: 10 },
  { metric: 'soilHum', title: '土壤湿度', unit: '%', valueMinLimit: 0, valueMaxLimit: 100, valueStep: 1, hysteresisMax: 20, hysteresisStep: 0.5 },
  { metric: 'ph', title: 'PH值', unit: '', valueMinLimit: 3, valueMaxLimit: 10, valueStep: 0.1, hysteresisMax: 1, hysteresisStep: 0.05 }
];

@Builder
export function WarningControlPageBuilder() {
  WarningControlPage();
}

@Component
struct WarningControlPage {
  pathStack: NavPathStack = new NavPathStack();
  private prefInstance: preferences.Preferences | null = null;
  private bypassLeaveConfirm: boolean = false;
  private lastSavedSnapshot: string = '';
  @State thresholds: ThresholdConfigModel = { items: [] };

  aboutToAppear(): void {
    this.thresholds = this.normalizeConfigModel(this.buildLocalConfigFromGlobal());
    this.lastSavedSnapshot = this.getConfigSnapshot(this.thresholds);
    this.loadFromPreferences();
  }

  build() {
    NavDestination() {
      List() {
        ListItem() {
          Column({ space: 16 }) {
            Text('智能预警配置')
              .fontSize(22)
              .fontWeight(FontWeight.Bold)
              .fontColor(Color.Black)

            Text('为各环境参数配置预警上下限，将根据阈值和实时数据自动给出预警提示。')
              .fontSize(13)
              .fontColor('#666666')

            Column({ space: 12 }) {
              ForEach(THRESHOLD_ROWS, (row: ThresholdRowViewModel, _index: number) => {
                this.renderThresholdRow(row);
              }, (row: ThresholdRowViewModel) => row.metric)
            }
            .width('100%')

            Row({ space: 16 }) {
              Button('取消')
                .layoutWeight(1)
                .height(40)
                .fontSize(16)
                .fontColor(Color.Black)
                .backgroundColor('#E0E0E0')
                .borderRadius(8)
                .onClick(() => {
                  this.thresholds = this.normalizeConfigModel(this.buildLocalConfigFromGlobal());
                })

              Button('保存')
                .layoutWeight(1)
                .height(40)
                .fontSize(16)
                .fontColor(Color.White)
                .backgroundColor('#2E7D32')
                .borderRadius(8)
                .onClick(() => {
                  this.handleSave(undefined, true);
                })
            }
            .width('100%')
            .padding({ top: 8, bottom: 8 })
          }
          .width('100%')
        }
      }
      .listDirection(Axis.Vertical)
      .edgeEffect(EdgeEffect.Spring)
      .width('100%')
      .height('100%')
      .padding({ left: 16, right: 16, top: 16, bottom: 16 })
      .backgroundColor('#F0F2F5')
    }
    .onReady((context: NavDestinationContext) => {
      this.pathStack = context.pathStack;
    })
    .onBackPressed((): boolean => {
      return this.handlePageLeaveRequest();
    })
  }

  @Builder
  renderThresholdRow(row: ThresholdRowViewModel) {
    Column({ space: 8 }) {
      Row({ space: 8 }) {
        Text(row.title)
          .fontSize(16)
          .fontWeight(FontWeight.Medium)
          .fontColor('#333333')

        Text(`预警范围：${this.formatValue(this.getItemMinValue(row.metric), row.valueStep)}${row.unit} ~ ${this.formatValue(this.getItemMaxValue(row.metric), row.valueStep)}${row.unit}`)
          .fontSize(12)
          .fontColor('#666666')
      }

      Column({ space: 8 }) {
        // 最小值
        Row({ space: 8 }) {
          Text('最小值')
            .fontSize(13)
            .fontColor('#666666')
          Slider({
            value: this.getItemMinValue(row.metric),
            min: row.valueMinLimit,
            max: row.valueMaxLimit,
            step: row.valueStep
          })
            .layoutWeight(1)
            .selectedColor('#2E7D32')
            .onChange((value: number) => {
              this.updateItemMinValue(row, value);
            })
          Text(`${this.formatValue(this.getItemMinValue(row.metric), row.valueStep)}${row.unit}`)
            .fontSize(13)
            .fontColor('#333333')
        }

        // 最大值
        Row({ space: 8 }) {
          Text('最大值')
            .fontSize(13)
            .fontColor('#666666')
          Slider({
            value: this.getItemMaxValue(row.metric),
            min: row.valueMinLimit,
            max: row.valueMaxLimit,
            step: row.valueStep
          })
            .layoutWeight(1)
            .selectedColor('#2E7D32')
            .onChange((value: number) => {
              this.updateItemMaxValue(row, value);
            })
          Text(`${this.formatValue(this.getItemMaxValue(row.metric), row.valueStep)}${row.unit}`)
            .fontSize(13)
            .fontColor('#333333')
        }

        // 回差 hysteresis
        Row({ space: 8 }) {
          Text('回差(防抖)')
            .fontSize(13)
            .fontColor('#666666')
          Slider({
            value: this.getItemHysteresis(row.metric),
            min: 0,
            max: row.hysteresisMax,
            step: row.hysteresisStep
          })
            .layoutWeight(1)
            .selectedColor('#2E7D32')
            .onChange((value: number) => {
              this.updateItemHysteresis(row, value);
            })
          Text(`${this.formatValue(this.getItemHysteresis(row.metric), row.hysteresisStep)}`)
            .fontSize(13)
            .fontColor('#333333')
        }
      }
      .width('100%')
    }
    .width('100%')
    .padding(12)
    .backgroundColor(Color.White)
    .borderRadius(8)
  }

  private getItemByMetric(metric: ThresholdMetric): ThresholdConfigItem {
    const exists: ThresholdConfigItem | undefined = this.thresholds.items.find((it: ThresholdConfigItem): boolean => it.metric === metric);
    if (exists) {
      return exists;
    }
    const seedGlobal: ThresholdConfigModel = getThresholdConfig();
    const seed: ThresholdConfigItem | undefined = seedGlobal.items.find((it: ThresholdConfigItem): boolean => it.metric === metric);
    if (seed) {
      return seed;
    }
    return {
      metric: metric,
      minValue: 0,
      maxValue: 100,
      hysteresis: 1
    };
  }

  private getItemMinValue(metric: ThresholdMetric): number {
    return this.getItemByMetric(metric).minValue;
  }

  private getItemMaxValue(metric: ThresholdMetric): number {
    return this.getItemByMetric(metric).maxValue;
  }

  private getItemHysteresis(metric: ThresholdMetric): number {
    return this.getItemByMetric(metric).hysteresis;
  }

  private clampValue(value: number, minValue: number, maxValue: number): number {
    if (value < minValue) {
      return minValue;
    }
    if (value > maxValue) {
      return maxValue;
    }
    return value;
  }

  private getPrecisionByStep(step: number): number {
    if (step >= 1) {
      return 0;
    }
    if (step >= 0.1) {
      return 1;
    }
    return 2;
  }

  private formatValue(value: number, step: number): string {
    const precision: number = this.getPrecisionByStep(step);
    return value.toFixed(precision);
  }

  private getRowByMetric(metric: ThresholdMetric): ThresholdRowViewModel {
    const row: ThresholdRowViewModel | undefined = THRESHOLD_ROWS.find((item: ThresholdRowViewModel): boolean => item.metric === metric);
    if (row) {
      return row;
    }
    return THRESHOLD_ROWS[0];
  }

  private normalizeItem(item: ThresholdConfigItem): ThresholdConfigItem {
    const row: ThresholdRowViewModel = this.getRowByMetric(item.metric);
    const minUpperLimit: number = row.valueMaxLimit - row.valueStep;
    const minValue: number = this.clampValue(item.minValue, row.valueMinLimit, minUpperLimit);
    const maxLowerLimit: number = minValue + row.valueStep;
    const maxValue: number = this.clampValue(item.maxValue, maxLowerLimit, row.valueMaxLimit);
    const hysteresis: number = this.clampValue(item.hysteresis, 0, row.hysteresisMax);
    return {
      metric: item.metric,
      minValue: minValue,
      maxValue: maxValue,
      hysteresis: hysteresis
    };
  }

  private normalizeConfigModel(config: ThresholdConfigModel): ThresholdConfigModel {
    const normalizedItems: ThresholdConfigItem[] = THRESHOLD_ROWS.map((row: ThresholdRowViewModel): ThresholdConfigItem => {
      const item: ThresholdConfigItem | undefined = config.items.find((it: ThresholdConfigItem): boolean => it.metric === row.metric);
      if (item) {
        return this.normalizeItem(item);
      }
      const defaultItem: ThresholdConfigItem = {
        metric: row.metric,
        minValue: row.valueMinLimit,
        maxValue: row.valueMinLimit + row.valueStep,
        hysteresis: 0
      };
      return defaultItem;
    });
    return { items: normalizedItems };
  }

  private getConfigSnapshot(config: ThresholdConfigModel): string {
    const normalized: ThresholdConfigModel = this.normalizeConfigModel(config);
    return JSON.stringify(normalized);
  }

  private hasUnsavedChanges(): boolean {
    return this.getConfigSnapshot(this.thresholds) !== this.lastSavedSnapshot;
  }

  private handlePageLeaveRequest(): boolean {
    if (this.bypassLeaveConfirm) {
      return false;
    }
    if (!this.hasUnsavedChanges()) {
      return false;
    }
    AlertDialog.show({
      title: '提示',
      message: '检测到你修改了预警阈值，离开前是否保存？',
      primaryButton: {
        value: '保存并返回',
        action: () => {
          this.handleSave(() => {
            this.navigateToControlPage();
          });
        }
      },
      secondaryButton: {
        value: '不保存返回',
        action: () => {
          this.navigateToControlPage();
        }
      }
    });
    // 拦截本次返回，等用户在弹窗中选择后再次触发返回
    return true;
  }

  private navigateToControlPage(): void {
    this.bypassLeaveConfirm = true;
    this.pathStack.pop();
  }

  private buildLocalConfigFromGlobal(): ThresholdConfigModel {
    const globalConfig: ThresholdConfigModel = getThresholdConfig();
    const nextItems: ThresholdConfigItem[] = globalConfig.items.map((item: ThresholdConfigItem): ThresholdConfigItem => {
      const nextItem: ThresholdConfigItem = {
        metric: item.metric,
        minValue: item.minValue,
        maxValue: item.maxValue,
        hysteresis: item.hysteresis
      };
      return nextItem;
    });
    return { items: nextItems };
  }

  private updateItemMinValue(row: ThresholdRowViewModel, value: number): void {
    const nextItems: ThresholdConfigItem[] = this.thresholds.items.map((item: ThresholdConfigItem): ThresholdConfigItem => {
      if (item.metric !== row.metric) {
        return item;
      }
      const minUpperLimit: number = Math.max(row.valueMinLimit, item.maxValue - row.valueStep);
      const minValue: number = this.clampValue(value, row.valueMinLimit, minUpperLimit);
      const nextItem: ThresholdConfigItem = {
        metric: item.metric,
        minValue: minValue,
        maxValue: item.maxValue,
        hysteresis: item.hysteresis
      };
      return nextItem;
    });
    this.thresholds = { items: nextItems };
  }

  private updateItemMaxValue(row: ThresholdRowViewModel, value: number): void {
    const nextItems: ThresholdConfigItem[] = this.thresholds.items.map((item: ThresholdConfigItem): ThresholdConfigItem => {
      if (item.metric !== row.metric) {
        return item;
      }
      const maxLowerLimit: number = Math.min(row.valueMaxLimit, item.minValue + row.valueStep);
      const maxValue: number = this.clampValue(value, maxLowerLimit, row.valueMaxLimit);
      const nextItem: ThresholdConfigItem = {
        metric: item.metric,
        minValue: item.minValue,
        maxValue: maxValue,
        hysteresis: item.hysteresis
      };
      return nextItem;
    });
    this.thresholds = { items: nextItems };
  }

  private updateItemHysteresis(row: ThresholdRowViewModel, value: number): void {
    const nextItems: ThresholdConfigItem[] = this.thresholds.items.map((item: ThresholdConfigItem): ThresholdConfigItem => {
      if (item.metric !== row.metric) {
        return item;
      }
      const nextItem: ThresholdConfigItem = {
        metric: item.metric,
        minValue: item.minValue,
        maxValue: item.maxValue,
        hysteresis: this.clampValue(value, 0, row.hysteresisMax)
      };
      return nextItem;
    });
    this.thresholds = { items: nextItems };
  }

  private async ensurePreferences(): Promise<preferences.Preferences | null> {
    if (this.prefInstance) {
      return this.prefInstance;
    }
    const context: common.UIAbilityContext = getContext(this) as common.UIAbilityContext;
    try {
      this.prefInstance = await preferences.getPreferences(context, THRESHOLD_PREF_NAME);
      return this.prefInstance;
    } catch (_err) {
      this.prefInstance = null;
      return null;
    }
  }

  private loadFromPreferences(): void {
    this.ensurePreferences().then((inst: preferences.Preferences | null) => {
      if (!inst) {
        return;
      }
      inst.get(THRESHOLD_PREF_KEY, '').then((text: Object) => {
        const raw: string = text as string;
        if (!raw || raw.length === 0) {
          return;
        }
        try {
          const parsed = JSON.parse(raw) as ThresholdConfigModel;
          if (parsed && parsed.items && parsed.items.length > 0) {
            this.thresholds = this.normalizeConfigModel(parsed);
            updateThresholdConfig(this.thresholds);
            this.lastSavedSnapshot = this.getConfigSnapshot(this.thresholds);
          }
        } catch (_e) {
        }
      }).catch((_e: Error) => {
      });
    }).catch((_e: Error) => {
    });
  }

  private handleSave(onDone?: () => void, showToast: boolean = false): void {
    const normalized: ThresholdConfigModel = this.normalizeConfigModel(this.thresholds);
    this.thresholds = normalized;
    updateThresholdConfig(normalized);
    this.lastSavedSnapshot = this.getConfigSnapshot(normalized);
    this.ensurePreferences().then((inst: preferences.Preferences | null) => {
      if (!inst) {
        if (showToast) {
          promptAction.showToast({ message: '保存成功' });
        }
        onDone?.();
        return;
      }
      const raw: string = JSON.stringify(normalized);
      inst.put(THRESHOLD_PREF_KEY, raw).then(() => {
        inst.flush().then(() => {
          if (showToast) {
            promptAction.showToast({ message: '保存成功' });
          }
          onDone?.();
        }).catch((_err: Error) => {
          if (showToast) {
            promptAction.showToast({ message: '保存成功' });
          }
          onDone?.();
        });
      }).catch((_e: Error) => {
        if (showToast) {
          promptAction.showToast({ message: '保存成功' });
        }
        onDone?.();
      });
    }).catch((_e: Error) => {
      if (showToast) {
        promptAction.showToast({ message: '保存成功' });
      }
      onDone?.();
    });
  }
}
