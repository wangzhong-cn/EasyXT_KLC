import type { ReactNode } from "react";

interface WorkbenchShellProps {
  title: string;
  subtitle: string;
  leftRail: ReactNode;
  mainPane: ReactNode;
  rightRail: ReactNode;
  bottomPane: ReactNode;
}

export function WorkbenchShell(props: WorkbenchShellProps) {
  return (
    <section className="workbench-shell">
      <header className="panel card workbench-header">
        <div>
          <h2>{props.title}</h2>
          <p>{props.subtitle}</p>
        </div>
        <div className="badge-group">
          <span className="badge badge-live">实时</span>
          <span className="badge">API 边界优先</span>
          <span className="badge">Qt legacy freeze</span>
        </div>
      </header>
      <div className="workbench-grid">
        <aside className="panel card left-rail">{props.leftRail}</aside>
        <main className="panel card main-pane">{props.mainPane}</main>
        <aside className="panel card right-rail">{props.rightRail}</aside>
      </div>
      <footer className="panel card bottom-pane">{props.bottomPane}</footer>
    </section>
  );
}