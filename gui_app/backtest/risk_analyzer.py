# -*- coding: utf-8 -*-
"""
风险分析器
计算各种风险指标和性能度量
"""

import numpy as np
from typing import Dict, List, Optional, Tuple

class RiskAnalyzer:
    """
    风险分析器
    
    功能特性：
    1. 计算各种风险指标（VaR、CVaR、最大回撤等）
    2. 性能度量（夏普比率、索提诺比率、卡尔马比率等）
    3. 交易统计分析
    4. 风险报告生成
    """
    
    def __init__(self, risk_free_rate: float = 0.03):
        """
        初始化风险分析器
        
        Args:
            risk_free_rate: 无风险利率（年化）
        """
        self.risk_free_rate = risk_free_rate
    
    def analyze_portfolio(self, 
                         portfolio_values: List[float], 
                         returns: Optional[List[float]] = None,
                         benchmark_returns: Optional[List[float]] = None) -> Dict[str, float]:
        """
        分析投资组合风险和性能
        
        Args:
            portfolio_values: 投资组合净值序列
            returns: 收益率序列（可选，会自动计算）
            benchmark_returns: 基准收益率序列（可选）
            
        Returns:
            风险分析结果字典
        """
        if len(portfolio_values) < 2:
            return self._get_default_metrics()
        
        # 计算收益率
        if returns is None:
            returns = self._calculate_returns(portfolio_values)
        
        # 基础统计
        total_return = (portfolio_values[-1] / portfolio_values[0]) - 1
        annualized_return = self._annualize_return(total_return, len(portfolio_values))
        volatility = self._calculate_volatility(returns)
        
        # 风险指标
        max_drawdown, max_dd_duration = self._calculate_max_drawdown(portfolio_values)
        var_95 = self._calculate_var(returns, confidence_level=0.05)
        cvar_95 = self._calculate_cvar(returns, confidence_level=0.05)
        
        # 性能指标
        sharpe_ratio = self._calculate_sharpe_ratio(returns, volatility)
        sortino_ratio = self._calculate_sortino_ratio(returns)
        calmar_ratio = self._calculate_calmar_ratio(annualized_return, max_drawdown)
        
        # 其他指标
        win_rate = self._calculate_win_rate(returns)
        profit_factor = self._calculate_profit_factor(returns)
        
        results = {
            # 收益指标
            'total_return': total_return,
            'annualized_return': annualized_return,
            'volatility': volatility,
            
            # 风险指标
            'max_drawdown': max_drawdown,
            'max_drawdown_duration': max_dd_duration,
            'var_95': var_95,
            'cvar_95': cvar_95,
            
            # 风险调整收益指标
            'sharpe_ratio': sharpe_ratio,
            'sortino_ratio': sortino_ratio,
            'calmar_ratio': calmar_ratio,
            
            # 交易统计
            'win_rate': win_rate,
            'profit_factor': profit_factor,
            
            # 其他指标
            'total_periods': len(portfolio_values),
            'positive_periods': sum(1 for r in returns if r > 0),
            'negative_periods': sum(1 for r in returns if r < 0)
        }
        
        # 如果有基准数据，计算相对指标
        if benchmark_returns is not None:
            benchmark_metrics = self._calculate_benchmark_metrics(returns, benchmark_returns)
            results.update(benchmark_metrics)
        
        return results
    
    def _calculate_returns(self, values: List[float]) -> List[float]:
        """计算收益率序列"""
        if len(values) < 2:
            return []
        
        returns = []
        for i in range(1, len(values)):
            if values[i-1] != 0:
                ret = (values[i] / values[i-1]) - 1
                returns.append(ret)
            else:
                returns.append(0.0)
        
        return returns
    
    def _annualize_return(self, total_return: float, periods: int, periods_per_year: int = 252) -> float:
        """年化收益率"""
        if periods <= 0:
            return 0.0
        
        years = periods / periods_per_year
        if years <= 0:
            return 0.0
        
        try:
            annualized = (1 + total_return) ** (1 / years) - 1
            return annualized
        except Exception:
            return 0.0
    
    def _calculate_volatility(self, returns: List[float], periods_per_year: int = 252) -> float:
        """计算年化波动率"""
        if len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        daily_vol = np.std(returns_array, ddof=1)
        annualized_vol = daily_vol * np.sqrt(periods_per_year)
        
        return annualized_vol
    
    def _calculate_max_drawdown(self, values: List[float]) -> Tuple[float, int]:
        """
        计算最大回撤和持续期
        
        Returns:
            (最大回撤比例, 最大回撤持续期)
        """
        if len(values) < 2:
            return 0.0, 0
        
        values_array = np.array(values)
        
        # 计算累计最高点
        peak = np.maximum.accumulate(values_array)
        
        # 计算回撤
        drawdown = (values_array - peak) / peak
        
        # 最大回撤
        max_dd = np.min(drawdown)
        
        # 计算最大回撤持续期
        max_dd_duration = 0
        current_dd_duration = 0
        
        for dd in drawdown:
            if dd < 0:
                current_dd_duration += 1
                max_dd_duration = max(max_dd_duration, current_dd_duration)
            else:
                current_dd_duration = 0
        
        return abs(max_dd), max_dd_duration
    
    def _calculate_var(self, returns: List[float], confidence_level: float = 0.05) -> float:
        """
        计算风险价值（VaR）
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平（如0.05表示95%置信度）
            
        Returns:
            VaR值（负数表示损失）
        """
        if len(returns) < 10:
            return 0.0
        
        returns_array = np.array(returns)
        var = np.percentile(returns_array, confidence_level * 100)
        
        return var
    
    def _calculate_cvar(self, returns: List[float], confidence_level: float = 0.05) -> float:
        """
        计算条件风险价值（CVaR）
        
        Args:
            returns: 收益率序列
            confidence_level: 置信水平
            
        Returns:
            CVaR值
        """
        if len(returns) < 10:
            return 0.0
        
        returns_array = np.array(returns)
        var = self._calculate_var(returns, confidence_level)
        
        # CVaR是超过VaR的损失的期望值
        tail_losses = returns_array[returns_array <= var]
        
        if len(tail_losses) > 0:
            cvar = np.mean(tail_losses)
        else:
            cvar = var
        
        return cvar
    
    def _calculate_sharpe_ratio(self, returns: List[float], volatility: float) -> float:
        """计算夏普比率"""
        if len(returns) < 2 or volatility == 0:
            return 0.0
        
        returns_array = np.array(returns)
        excess_return = np.mean(returns_array) * 252 - self.risk_free_rate  # 年化超额收益
        
        sharpe = excess_return / volatility if volatility != 0 else 0.0
        
        return sharpe
    
    def _calculate_sortino_ratio(self, returns: List[float]) -> float:
        """计算索提诺比率"""
        if len(returns) < 2:
            return 0.0
        
        returns_array = np.array(returns)
        
        # 计算下行波动率
        negative_returns = returns_array[returns_array < 0]
        if len(negative_returns) > 0:
            downside_deviation = np.std(negative_returns, ddof=1) * np.sqrt(252)
        else:
            downside_deviation = 0.0
        
        if downside_deviation == 0:
            return float('inf') if np.mean(returns_array) > 0 else 0.0
        
        excess_return = np.mean(returns_array) * 252 - self.risk_free_rate
        sortino = excess_return / downside_deviation
        
        return sortino
    
    def _calculate_calmar_ratio(self, annualized_return: float, max_drawdown: float) -> float:
        """计算卡尔马比率"""
        if max_drawdown == 0:
            return float('inf') if annualized_return > 0 else 0.0
        
        calmar = annualized_return / max_drawdown
        return calmar
    
    def _calculate_win_rate(self, returns: List[float]) -> float:
        """计算胜率"""
        if len(returns) == 0:
            return 0.0
        
        positive_returns = sum(1 for r in returns if r > 0)
        win_rate = positive_returns / len(returns)
        
        return win_rate
    
    def _calculate_profit_factor(self, returns: List[float]) -> float:
        """计算盈利因子"""
        if len(returns) == 0:
            return 0.0
        
        returns_array = np.array(returns)
        
        gross_profit = np.sum(returns_array[returns_array > 0])
        gross_loss = abs(np.sum(returns_array[returns_array < 0]))
        
        if gross_loss == 0:
            return float('inf') if gross_profit > 0 else 0.0
        
        profit_factor = gross_profit / gross_loss
        return profit_factor
    
    def _calculate_benchmark_metrics(self, returns: List[float], benchmark_returns: List[float]) -> Dict[str, float]:
        """计算相对基准的指标"""
        if len(returns) != len(benchmark_returns) or len(returns) < 2:
            return {}
        
        returns_array = np.array(returns)
        benchmark_array = np.array(benchmark_returns)
        
        # 超额收益
        excess_returns = returns_array - benchmark_array
        
        # 信息比率
        if len(excess_returns) > 1:
            tracking_error = np.std(excess_returns, ddof=1) * np.sqrt(252)
            information_ratio = np.mean(excess_returns) * 252 / tracking_error if tracking_error != 0 else 0.0
        else:
            information_ratio = 0.0
            tracking_error = 0.0
        
        # Beta系数
        if np.std(benchmark_array) != 0:
            beta = np.cov(returns_array, benchmark_array)[0, 1] / np.var(benchmark_array)
        else:
            beta = 0.0
        
        # Alpha
        benchmark_return = np.mean(benchmark_array) * 252
        portfolio_return = np.mean(returns_array) * 252
        alpha = portfolio_return - (self.risk_free_rate + beta * (benchmark_return - self.risk_free_rate))
        
        return {
            'alpha': alpha,
            'beta': beta,
            'information_ratio': information_ratio,
            'tracking_error': tracking_error
        }
    
    def _get_default_metrics(self) -> Dict[str, float]:
        """返回默认指标（当数据不足时）"""
        return {
            'total_return': 0.0,
            'annualized_return': 0.0,
            'volatility': 0.0,
            'max_drawdown': 0.0,
            'max_drawdown_duration': 0,
            'var_95': 0.0,
            'cvar_95': 0.0,
            'sharpe_ratio': 0.0,
            'sortino_ratio': 0.0,
            'calmar_ratio': 0.0,
            'win_rate': 0.0,
            'profit_factor': 0.0,
            'total_periods': 0,
            'positive_periods': 0,
            'negative_periods': 0
        }
    
    def generate_risk_report(self, analysis_results: Dict[str, float]) -> str:
        """
        生成风险分析报告
        
        Args:
            analysis_results: 分析结果字典
            
        Returns:
            格式化的风险报告文本
        """
        report = []
        report.append("=" * 50)
        report.append("[CHART] 投资组合风险分析报告")
        report.append("=" * 50)
        
        # 收益指标
        report.append("\n[UP] 收益指标:")
        report.append(f"  总收益率: {analysis_results.get('total_return', 0):.2%}")
        report.append(f"  年化收益率: {analysis_results.get('annualized_return', 0):.2%}")
        report.append(f"  年化波动率: {analysis_results.get('volatility', 0):.2%}")
        
        # 风险指标
        report.append("\n[WARNING] 风险指标:")
        report.append(f"  最大回撤: {analysis_results.get('max_drawdown', 0):.2%}")
        report.append(f"  最大回撤持续期: {analysis_results.get('max_drawdown_duration', 0)} 期")
        report.append(f"  95% VaR: {analysis_results.get('var_95', 0):.2%}")
        report.append(f"  95% CVaR: {analysis_results.get('cvar_95', 0):.2%}")
        
        # 风险调整收益指标
        report.append("\n[TARGET] 风险调整收益指标:")
        report.append(f"  夏普比率: {analysis_results.get('sharpe_ratio', 0):.3f}")
        report.append(f"  索提诺比率: {analysis_results.get('sortino_ratio', 0):.3f}")
        report.append(f"  卡尔马比率: {analysis_results.get('calmar_ratio', 0):.3f}")
        
        # 交易统计
        report.append("\n[CHART] 交易统计:")
        report.append(f"  胜率: {analysis_results.get('win_rate', 0):.2%}")
        report.append(f"  盈利因子: {analysis_results.get('profit_factor', 0):.2f}")
        report.append(f"  总交易期数: {analysis_results.get('total_periods', 0)}")
        
        # 基准比较（如果有）
        if 'alpha' in analysis_results:
            report.append("\n[UP] 基准比较:")
            report.append(f"  Alpha: {analysis_results.get('alpha', 0):.2%}")
            report.append(f"  Beta: {analysis_results.get('beta', 0):.3f}")
            report.append(f"  信息比率: {analysis_results.get('information_ratio', 0):.3f}")
            report.append(f"  跟踪误差: {analysis_results.get('tracking_error', 0):.2%}")
        
        # 风险评级
        report.append("\n🏆 风险评级:")
        risk_level = self._assess_risk_level(analysis_results)
        report.append(f"  风险等级: {risk_level}")
        
        report.append("\n" + "=" * 50)
        
        return "\n".join(report)
    
    def _assess_risk_level(self, metrics: Dict[str, float]) -> str:
        """评估风险等级"""
        sharpe = metrics.get('sharpe_ratio', 0)
        max_dd = metrics.get('max_drawdown', 0)
        volatility = metrics.get('volatility', 0)
        
        # 综合评分
        score = 0
        
        # 夏普比率评分
        if sharpe > 2.0:
            score += 3
        elif sharpe > 1.0:
            score += 2
        elif sharpe > 0.5:
            score += 1
        
        # 最大回撤评分
        if max_dd < 0.05:
            score += 3
        elif max_dd < 0.10:
            score += 2
        elif max_dd < 0.20:
            score += 1
        
        # 波动率评分
        if volatility < 0.10:
            score += 3
        elif volatility < 0.20:
            score += 2
        elif volatility < 0.30:
            score += 1
        
        # 根据总分确定等级
        if score >= 7:
            return "[GREEN] 低风险 (优秀)"
        elif score >= 5:
            return "[YELLOW] 中等风险 (良好)"
        elif score >= 3:
            return "🟠 较高风险 (一般)"
        else:
            return "[RED] 高风险 (需要改进)"


if __name__ == "__main__":
    # 测试风险分析器
    analyzer = RiskAnalyzer()
    
    # 生成测试数据
    np.random.seed(42)
    initial_value = 100000
    returns = np.random.normal(0.001, 0.02, 252)  # 一年的日收益率
    
    portfolio_values = [initial_value]
    for ret in returns:
        new_value = portfolio_values[-1] * (1 + ret)
        portfolio_values.append(new_value)
    
    # 分析风险
    results = analyzer.analyze_portfolio(portfolio_values)
    
    # 生成报告
    report = analyzer.generate_risk_report(results)
    print(report)
