"""
绘图工具函数
提供因子分析所需的可视化功能
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
from typing import Dict, List, Optional
import sys
import os

# 设置中文字体支持
plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False


def create_factor_ic_plot(ic_series, factor_name: str = "Factor", save_path: Optional[str] = None):
    """
    创建因子IC图

    Args:
        ic_series: IC序列
        factor_name: 因子名称
        save_path: 保存路径
    """
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    # IC时序图
    axes[0].plot(ic_series.index, ic_series.values, linewidth=1.0)
    axes[0].axhline(y=0, color='r', linestyle='--', alpha=0.5, label='Zero')
    axes[0].set_title(f'{factor_name} Information Coefficient Over Time')
    axes[0].set_ylabel('IC')
    axes[0].grid(True, alpha=0.3)
    axes[0].legend()

    # IC分布直方图
    ic_clean = ic_series.dropna()
    axes[1].hist(ic_clean, bins=50, density=True, alpha=0.7, edgecolor='black')
    axes[1].axvline(x=ic_clean.mean(), color='r', linestyle='--',
                    label=f'Mean: {ic_clean.mean():.4f}')
    axes[1].axvline(x=0, color='k', linestyle='-', alpha=0.5, label='Zero')
    axes[1].set_title(f'{factor_name} IC Distribution')
    axes[1].set_xlabel('IC Value')
    axes[1].set_ylabel('Density')
    axes[1].grid(True, alpha=0.3)
    axes[1].legend()

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


def create_factor_return_plot(return_series, factor_name: str = "Factor", save_path: Optional[str] = None):
    """
    创建因子收益图

    Args:
        return_series: 收益序列
        factor_name: 因子名称
        save_path: 保存路径
    """
    fig, axes = plt.subplots(2, 1, figsize=(12, 10))

    # 累计收益图
    cum_return = (1 + return_series).cumprod()
    axes[0].plot(cum_return.index, cum_return.values, linewidth=1.5)
    axes[0].set_title(f'{factor_name} Cumulative Return')
    axes[0].set_ylabel('Cumulative Return')
    axes[0].grid(True, alpha=0.3)

    # 收益分布直方图
    return_clean = return_series.dropna()
    axes[1].hist(return_clean, bins=50, density=True, alpha=0.7, edgecolor='black')
    axes[1].axvline(x=return_clean.mean(), color='r', linestyle='--',
                    label=f'Mean: {return_clean.mean():.4f}')
    axes[1].set_title(f'{factor_name} Return Distribution')
    axes[1].set_xlabel('Return')
    axes[1].set_ylabel('Density')
    axes[1].grid(True, alpha=0.3)
    axes[1].legend()

    plt.tight_layout()

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


def create_quantile_analysis_plot(quantile_returns: Dict[str, pd.Series], save_path: Optional[str] = None):
    """
    创建分层分析图

    Args:
        quantile_returns: 各分层的收益序列
        save_path: 保存路径
    """
    plt.figure(figsize=(12, 8))

    for q_name, returns in quantile_returns.items():
        cum_return = (1 + returns).cumprod()
        plt.plot(cum_return.index, cum_return.values, label=q_name, linewidth=1.5)

    plt.title('Quantile Portfolio Cumulative Return Comparison')
    plt.xlabel('Date')
    plt.ylabel('Cumulative Return')
    plt.legend()
    plt.grid(True, alpha=0.3)

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


def create_factor_scatter_plot(factor_values: pd.Series, target_values: pd.Series,
                              factor_name: str = "Factor", target_name: str = "Target",
                              save_path: Optional[str] = None):
    """
    创建因子散点图

    Args:
        factor_values: 因子值
        target_values: 目标值
        factor_name: 因子名称
        target_name: 目标名称
        save_path: 保存路径
    """
    # 对齐数据
    aligned_factor, aligned_target = factor_values.align(target_values, join='inner')

    plt.figure(figsize=(10, 8))
    plt.scatter(aligned_factor, aligned_target, alpha=0.6)

    # 添加趋势线
    z = np.polyfit(aligned_factor, aligned_target, 1)
    p = np.poly1d(z)
    plt.plot(aligned_factor, p(aligned_factor), "r--", alpha=0.8, linewidth=2)

    # 计算相关系数
    correlation = aligned_factor.corr(aligned_target)

    plt.title(f'{factor_name} vs {target_name}\nCorrelation: {correlation:.4f}')
    plt.xlabel(factor_name)
    plt.ylabel(target_name)
    plt.grid(True, alpha=0.3)

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


def create_turnover_heatmap(turnover_matrix: pd.DataFrame, save_path: Optional[str] = None):
    """
    创建换手率热力图

    Args:
        turnover_matrix: 换手率矩阵
        save_path: 保存路径
    """
    plt.figure(figsize=(12, 8))
    sns.heatmap(turnover_matrix, annot=True, fmt='.3f', cmap='RdBu_r', center=0,
                cbar_kws={'label': 'Turnover Rate'})
    plt.title('Factor Turnover Heatmap')
    plt.xlabel('Target Period')
    plt.ylabel('Factor Period')

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


def create_performance_summary_table(metrics: Dict[str, float], save_path: Optional[str] = None):
    """
    创建绩效汇总表

    Args:
        metrics: 绩效指标字典
        save_path: 保存路径
    """
    # 创建汇总表
    df = pd.DataFrame(list(metrics.items()), columns=['Metric', 'Value'])
    df['Value'] = df['Value'].round(4)

    # 创建表格图像
    fig, ax = plt.subplots(figsize=(8, len(metrics) * 0.3 + 1))
    ax.axis('tight')
    ax.axis('off')

    table = ax.table(cellText=df.values, colLabels=df.columns, cellLoc='center', loc='center')
    table.auto_set_font_size(False)
    table.set_fontsize(10)
    table.scale(1.2, 1.5)

    for i in range(len(df) + 1):
        for j in range(2):
            cell = table[(i, j)]
            if i == 0:  # 表头
                cell.set_facecolor('#4CAF50')
                cell.set_text_props(weight='bold', color='white')
            else:
                if j == 0:  # 指标名
                    cell.set_facecolor('#f0f0f0')
                else:  # 数值
                    cell.set_facecolor('#ffffff')

    plt.title('Performance Summary', pad=20)

    if save_path:
        plt.savefig(save_path, dpi=300, bbox_inches='tight')

    plt.show()


# 测试代码
if __name__ == '__main__':
    print("plot_utils.py 需要提供真实市场数据运行，请通过 EasyXT 数据接口加载数据后调用。")