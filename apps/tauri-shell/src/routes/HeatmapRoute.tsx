export function HeatmapRoute() {
  const blocks = [
    ["金融", "2.30%", "positive"],
    ["新能源", "-0.76%", "negative"],
    ["消费", "0.95%", "positive"],
    ["科技", "-2.36%", "negative"],
    ["油气", "3.07%", "positive"],
    ["通信", "-0.02%", "negative"],
    ["军工", "1.32%", "positive"],
    ["医药", "1.05%", "positive"],
  ] as const;

  return (
    <section className="route-stack">
      <header className="panel card route-header">
        <h2>热力图视图</h2>
        <p>参考同花顺 / 东方财富的大盘云图与板块热区：独立路由，不和交易主工作台混在一起。</p>
      </header>
      <div className="panel card heatmap-grid">
        {blocks.map(([title, value, tone]) => (
          <article key={title} className={`heatmap-block ${tone}`}>
            <h3>{title}</h3>
            <strong>{value}</strong>
          </article>
        ))}
      </div>
    </section>
  );
}