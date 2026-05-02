import type { ChartInterval } from "../../chart/chartFacadeV2";

export interface WorkbenchChartPanelConfig {
  id: string;
  title: string;
  subtitle: string;
  interval: ChartInterval;
  eager: boolean;
}

export interface WorkbenchChartLayoutConfig {
  presetId: string;
  slotCount: 1 | 2 | 4;
  layoutClassName: "single" | "dual" | "quad";
  panels: WorkbenchChartPanelConfig[];
}

const INTERVAL_LADDER: ChartInterval[] = ["1m", "5m", "15m", "30m", "1h", "4h", "1d", "1w"];

function uniqueIntervals(intervals: ChartInterval[]): ChartInterval[] {
  return Array.from(new Set(intervals));
}

function buildFourIntervalCluster(baseInterval: ChartInterval): ChartInterval[] {
  switch (baseInterval) {
    case "1m":
    case "5m":
      return ["1m", "5m", "15m", "1h"];
    case "15m":
    case "30m":
      return ["5m", "15m", "30m", "1h"];
    case "1h":
    case "4h":
      return ["15m", "30m", "1h", "4h"];
    case "1w":
      return ["1h", "4h", "1d", "1w"];
    case "1d":
    default:
      return ["15m", "1h", "1d", "1w"];
  }
}

function getHigherInterval(baseInterval: ChartInterval): ChartInterval {
  const currentIndex = INTERVAL_LADDER.indexOf(baseInterval);
  if (currentIndex < 0) {
    return "1d";
  }
  return INTERVAL_LADDER[Math.min(currentIndex + 2, INTERVAL_LADDER.length - 1)];
}

function createSingleLayout(presetId: string, baseInterval: ChartInterval): WorkbenchChartLayoutConfig {
  return {
    presetId,
    slotCount: 1,
    layoutClassName: "single",
    panels: [
      {
        id: "pane-main",
        title: presetId === "研究布局" ? "研究主图" : "主图舞台",
        subtitle: presetId === "专业布局（右）"
          ? "专业主图 / 交易侧栏联动"
          : presetId === "研究布局"
            ? "聚焦结构与标注读链路"
            : "单图主舞台",
        interval: baseInterval,
        eager: true,
      },
    ],
  };
}

export function resolveWorkbenchChartLayout(
  presetId: string,
  baseInterval: ChartInterval,
): WorkbenchChartLayoutConfig {
  if (presetId === "双图") {
    const higherInterval = getHigherInterval(baseInterval);
    const intervals = uniqueIntervals([baseInterval, higherInterval]);
    return {
      presetId,
      slotCount: 2,
      layoutClassName: "dual",
      panels: intervals.slice(0, 2).map((interval, index) => ({
        id: `pane-dual-${index + 1}`,
        title: index === 0 ? "主视图" : "高阶视图",
        subtitle: index === 0 ? "当前工作周期" : "上级确认周期",
        interval,
        eager: index === 0,
      })),
    };
  }

  if (presetId === "四图") {
    const intervals = buildFourIntervalCluster(baseInterval);
    return {
      presetId,
      slotCount: 4,
      layoutClassName: "quad",
      panels: intervals.map((interval, index) => ({
        id: `pane-quad-${index + 1}`,
        title: `${interval} 视图`,
        subtitle: index === 0 ? "快速切片" : index === intervals.length - 1 ? "高阶确认" : "跨周期联动",
        interval,
        eager: index === 0,
      })),
    };
  }

  return createSingleLayout(presetId, baseInterval);
}
