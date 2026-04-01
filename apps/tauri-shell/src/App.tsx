import { useEffect, useMemo, useState } from "react";

import { PRIMARY_ROUTES, TOP_NAV_ITEMS, type AppRouteKey } from "./lib/navigation";
import { getAppRouteEventName } from "./lib/routeBridge";
import { WorkbenchRoute } from "./routes/WorkbenchRoute";
import { StructureRoute } from "./routes/StructureRoute";
import { DataRoute } from "./routes/DataRoute";
import { HeatmapRoute } from "./routes/HeatmapRoute";
import { IdeasRoute } from "./routes/IdeasRoute";
import { SystemRoute } from "./routes/SystemRoute";

export default function App() {
  const [route, setRoute] = useState<AppRouteKey>("workbench");

  useEffect(() => {
    function handleRouteEvent(event: Event) {
      const detail = (event as CustomEvent<{ route?: AppRouteKey }>).detail;
      if (!detail?.route) {
        return;
      }
      setRoute(detail.route);
    }
    window.addEventListener(getAppRouteEventName(), handleRouteEvent as EventListener);
    return () => {
      window.removeEventListener(getAppRouteEventName(), handleRouteEvent as EventListener);
    };
  }, []);

  const routeElement = useMemo(() => {
    switch (route) {
      case "data":
        return <DataRoute />;
      case "structure":
        return <StructureRoute />;
      case "heatmap":
        return <HeatmapRoute />;
      case "ideas":
        return <IdeasRoute />;
      case "system":
        return <SystemRoute />;
      case "workbench":
      default:
        return <WorkbenchRoute />;
    }
  }, [route]);

  return (
    <div className="app-frame">
      <header className="top-header">
        <div className="brand-block">
          <strong>EasyXT</strong>
          <span>Tauri Shell P0</span>
        </div>
        <nav className="top-nav">
          {TOP_NAV_ITEMS.map((item) => (
            <button key={item} type="button" className="ghost-button">
              {item}
            </button>
          ))}
        </nav>
        <div className="status-rail">
          <span className="status-dot" />
          <span>API only / Qt freeze</span>
        </div>
      </header>

      <div className="page-frame">
        <aside className="primary-side-nav panel card">
          <h1>新壳导航</h1>
          <p>目标：所有新增可视化能力都进入这里，而不是回流到 `gui_app/`。</p>
          <div className="route-list">
            {PRIMARY_ROUTES.map((item) => (
              <button
                key={item.key}
                type="button"
                className={`route-button ${route === item.key ? "active" : ""}`}
                onClick={() => setRoute(item.key)}
              >
                <strong>{item.title}</strong>
                <span>{item.subtitle}</span>
              </button>
            ))}
          </div>
        </aside>

        <section className="content-stage">{routeElement}</section>
      </div>
    </div>
  );
}
