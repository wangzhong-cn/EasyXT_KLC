import { createFallbackChartFacade } from "./adapters/fallbackAdapter";
import {
  createProChartFacade,
  getProAdapterDescriptor,
  isProAdapterReady,
} from "./adapters/proAdapter";
import type { ChartFacadeBundle, ChartFacadeFactoryOptions } from "./chartFacadeV2";

export function createChartFacadeV2(options: ChartFacadeFactoryOptions): ChartFacadeBundle {
  const preferredAdapter = options.preferredAdapter ?? "pro";

  if (preferredAdapter === "pro") {
    const requestedAdapter = getProAdapterDescriptor();

    if (isProAdapterReady()) {
      const facade = createProChartFacade({
        ...options,
        requestedAdapter,
      });
      return {
        facade,
        activeAdapter: facade.getAdapterDescriptor(),
        requestedAdapter,
        fallbackEngaged: false,
      };
    }

    const facade = createFallbackChartFacade({
      ...options,
      requestedAdapter,
    });

    return {
      facade,
      activeAdapter: facade.getAdapterDescriptor(),
      requestedAdapter,
      fallbackEngaged: true,
    };
  }

  const facade = createFallbackChartFacade(options);
  const activeAdapter = facade.getAdapterDescriptor();

  return {
    facade,
    activeAdapter,
    requestedAdapter: activeAdapter,
    fallbackEngaged: false,
  };
}
