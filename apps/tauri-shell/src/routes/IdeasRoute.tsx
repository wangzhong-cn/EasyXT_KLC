export function IdeasRoute() {
  const cards = [
    {
      title: "交易观点",
      text: "对齐 TradingHero / 社区卡片流：研究内容、脚本摘要和市场快评应成为独立路由。",
    },
    {
      title: "脚本市场",
      text: "脚本与策略入口不再藏进旧 Qt 面板，未来由 Web/Tauri 提供统一浏览与安装位。",
    },
    {
      title: "复盘与计划",
      text: "研究与执行分离，避免交易工作台在同一屏里承担过多非实时信息密度。",
    },
  ];

  return (
    <section className="route-stack">
      <header className="panel card route-header">
        <h2>观点 / 社区 / 脚本</h2>
        <p>参考 TradingHero 的卡片流，把内容层和交易层拆开，防止交易终端过载。</p>
      </header>
      <div className="idea-grid">
        {cards.map((card) => (
          <article key={card.title} className="panel card idea-card">
            <h3>{card.title}</h3>
            <p>{card.text}</p>
            <footer>
              <span>👍 12</span>
              <span>💬 4</span>
            </footer>
          </article>
        ))}
      </div>
    </section>
  );
}