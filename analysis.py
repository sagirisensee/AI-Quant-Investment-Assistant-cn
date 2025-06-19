import asyncio
import logging
import random
import pandas as pd
import pandas_ta as ta
from collections import deque
from ak_utils import (
    get_all_etf_spot_realtime, get_etf_daily_history, CORE_ETF_POOL,
    get_all_stock_spot_realtime, get_stock_daily_history, CORE_STOCK_POOL
)
from llm_analyzer import get_llm_score_and_analysis


logger = logging.getLogger(__name__)

async def generate_ai_driven_report(get_realtime_data_func, get_daily_history_func, core_pool):
    """
    融合量化分析与LLM分析，生成最终报告的统一函数。
    参数:
        get_realtime_data_func: 获取实时数据的函数 (例如 get_all_etf_spot_realtime 或 get_all_stock_spot_realtime)
        get_daily_history_func: 获取历史日线数据的函数 (例如 get_etf_daily_history 或 get_stock_daily_history)
        core_pool: 核心观察池 (例如 CORE_ETF_POOL 或 CORE_STOCK_POOL)
    """
    logger.info("启动AI驱动的统一全面分析引擎...")
    
    # 并行获取实时数据和日线趋势
    realtime_data_df_task = asyncio.to_thread(get_realtime_data_func)
    daily_trends_task = _get_daily_trends_generic(get_daily_history_func, core_pool)
    
    realtime_data_df, daily_trends_list = await asyncio.gather(realtime_data_df_task, daily_trends_task)
    
    if realtime_data_df is None:
        return [{"name": "错误", "code": "", "ai_score": 0, "ai_comment": "获取实时数据失败，无法分析。"}]
    
    daily_trends_map = {item['code']: item for item in daily_trends_list}
    
    # 复用盘中信号生成器，传入当前使用的观察池
    intraday_analyzer = _IntradaySignalGenerator(core_pool)
    intraday_signals = intraday_analyzer.generate_signals(realtime_data_df)
    
    final_report = []
    for i, signal in enumerate(intraday_signals):
        code = signal['code']
        name = signal['name']
        logger.info(f"正在调用LLM分析: {name} ({i+1}/{len(intraday_signals)})")
        try:
            daily_trend = daily_trends_map.get(code, {'status': '未知'})
            # 复用LLM分析器
            ai_score, ai_comment = await get_llm_score_and_analysis(signal, daily_trend)
            final_report.append({
                **signal,
                "ai_score": ai_score if ai_score is not None else 0,
                "ai_comment": ai_comment
            })
        except Exception as e:
            logger.error(f"处理LLM分析 {name} 时发生错误: {e}")
            final_report.append({**signal, "ai_score": 0, "ai_comment": "处理时发生未知错误。"})
        # 保持礼貌的请求间隔
        await asyncio.sleep(random.uniform(1.0, 2.5))
    
    return sorted(final_report, key=lambda x: x.get('ai_score', 0), reverse=True)



async def _get_daily_trends_generic(get_daily_history_func, core_pool):
    """
    获取指定观察池中所有项目的日线趋势和详细技术指标。
    参数:
        get_daily_history_func: 获取历史日线数据的函数
        core_pool: 核心观察池
    """
    analysis_report = []
    for item_info in core_pool:
        try:
            result = await get_daily_history_func(item_info['code'])
            if result is None or result.empty:
                analysis_report.append({**item_info, 'status': '🟡 数据不足', 'technical_indicators_summary': []})
                continue
            
            # 确保数据足够计算指标
            if len(result) < 60: # 至少需要60条数据才能计算60日均线
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于60天)', 'technical_indicators_summary': []})
                continue

            # 计算所有需要的指标
            result.ta.sma(close='收盘', length=5, append=True)
            result.ta.sma(close='收盘', length=10, append=True)
            result.ta.sma(close='收盘', length=20, append=True)
            result.ta.sma(close='收盘', length=60, append=True)
            
            # 计算MACD，pandas_ta默认参数为 fast=12, slow=26, signal=9
            macd_data = result.ta.macd(close='收盘', append=True) 

            # 获取最新和倒数第二日的数据
            latest = result.iloc[-1]
            prev_latest = result.iloc[-2]

            trend_signals = [] # 用于存储本次分析提取的详细技术信号

            # ----------------------------------------------------
            # 1. 移动平均线（MA）关系
            # ----------------------------------------------------
            # 股价与均线关系
            if 'SMA_5' in latest and pd.notna(latest['SMA_5']) and latest['收盘'] > latest['SMA_5']: trend_signals.append("股价高于5日均线")
            if 'SMA_10' in latest and pd.notna(latest['SMA_10']) and latest['收盘'] > latest['SMA_10']: trend_signals.append("股价高于10日均线")
            if 'SMA_20' in latest and pd.notna(latest['SMA_20']) and latest['收盘'] > latest['SMA_20']: trend_signals.append("股价高于20日均线")
            if 'SMA_60' in latest and pd.notna(latest['SMA_60']) and latest['收盘'] > latest['SMA_60']: trend_signals.append("股价高于60日均线")

            # 均线交叉（金叉/死叉 - 需要判断前一日状态）
            if 'SMA_5' in latest and 'SMA_10' in latest and pd.notna(latest['SMA_5']) and pd.notna(latest['SMA_10']):
                if latest['SMA_5'] > latest['SMA_10'] and prev_latest['SMA_5'] <= prev_latest['SMA_10']:
                    trend_signals.append("5日均线金叉10日均线 (短期看涨)")
                elif latest['SMA_5'] < latest['SMA_10'] and prev_latest['SMA_5'] >= prev_latest['SMA_10']:
                    trend_signals.append("5日均线死叉10日均线 (短期看跌)")
            
            if 'SMA_10' in latest and 'SMA_20' in latest and pd.notna(latest['SMA_10']) and pd.notna(latest['SMA_20']):
                if latest['SMA_10'] > latest['SMA_20'] and prev_latest['SMA_10'] <= prev_latest['SMA_20']:
                    trend_signals.append("10日均线金叉20日均线 (短期强势)")
                elif latest['SMA_10'] < latest['SMA_20'] and prev_latest['SMA_10'] >= prev_latest['SMA_20']:
                    trend_signals.append("10日均线死叉20日均线 (短期走弱)")

            if 'SMA_20' in latest and 'SMA_60' in latest and pd.notna(latest['SMA_20']) and pd.notna(latest['SMA_60']):
                if latest['SMA_20'] > latest['SMA_60'] and prev_latest['SMA_20'] <= prev_latest['SMA_60']:
                    trend_signals.append("20日均线金叉60日均线 (中长期金叉)")
                elif latest['SMA_20'] < latest['SMA_60'] and prev_latest['SMA_20'] >= prev_latest['SMA_60']:
                    trend_signals.append("20日均线死叉60日均线 (中长期死叉)")

            # 60日均线趋势
            if 'SMA_60' in latest and 'SMA_60' in prev_latest and pd.notna(latest['SMA_60']) and pd.notna(prev_latest['SMA_60']):
                if latest['SMA_60'] > prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向上 (中长期趋势积极)")
                elif latest['SMA_60'] < prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向下 (中长期趋势谨慎)")

            # ----------------------------------------------------
            # 2. MACD 指标
            # ----------------------------------------------------
            # pandas_ta 生成的MACD列名通常是 MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9
            macd_line_col = 'MACD_12_26_9'
            signal_line_col = 'MACDs_12_26_9'
            histogram_col = 'MACDh_12_26_9'

            if macd_line_col in latest and signal_line_col in latest and histogram_col in latest and \
               pd.notna(latest[macd_line_col]) and pd.notna(latest[signal_line_col]):

                # MACD金叉/死叉信号
                if latest[macd_line_col] > latest[signal_line_col] and prev_latest[macd_line_col] <= prev_latest[signal_line_col]:
                    trend_signals.append("MACD金叉 (看涨信号)")
                elif latest[macd_line_col] < latest[signal_line_col] and prev_latest[macd_line_col] >= prev_latest[signal_line_col]:
                    trend_signals.append("MACD死叉 (看跌信号)")
                
                # MACD线与零轴关系
                if latest[macd_line_col] > 0:
                    trend_signals.append("MACD线在零轴上方 (强势区域)")
                else:
                    trend_signals.append("MACD线在零轴下方 (弱势区域)")
                
                # MACD柱线变化
                if pd.notna(latest[histogram_col]) and pd.notna(prev_latest[histogram_col]):
                    if latest[histogram_col] > 0 and latest[histogram_col] > prev_latest[histogram_col]:
                        trend_signals.append("MACD红柱增长 (多头力量增强)")
                    elif latest[histogram_col] < 0 and latest[histogram_col] < prev_latest[histogram_col]:
                        trend_signals.append("MACD绿柱增长 (空头力量增强)")
                    elif latest[histogram_col] > 0 and latest[histogram_col] < prev_latest[histogram_col]:
                        trend_signals.append("MACD红柱缩短 (多头力量减弱)")
                    elif latest[histogram_col] < 0 and latest[histogram_col] > prev_latest[histogram_col]:
                        trend_signals.append("MACD绿柱缩短 (空头力量减弱)")
            
            # ----------------------------------------------------
            # 最终报告状态
            # ----------------------------------------------------
            # 这里的status可以继续保持基于20日均线的判断，或者根据需要调整。
            status = '🟢 上升趋势' if 'SMA_20' in latest and pd.notna(latest['SMA_20']) and latest['收盘'] > latest['SMA_20'] else '🔴 下降趋势'

            analysis_report.append({
                **item_info,
                'status': status, # 保持原有的整体趋势
                'technical_indicators_summary': trend_signals # 传递详细的技术指标分析
            })
        except Exception as e:
            logger.error(f"❌ 分析 {item_info.get('name', item_info['code'])} 日线数据时失败: {e}", exc_info=True)
            analysis_report.append({**item_info, 'status': '❌ 分析失败', 'technical_indicators_summary': []})
        await asyncio.sleep(random.uniform(1.0, 2.0))
    return analysis_report

class _IntradaySignalGenerator:
    """内部辅助类：生成盘中量化信号 (带相对成交量)"""
    def __init__(self, item_list): # 更改 etf_list 为更通用的 item_list
        self.item_list = item_list
        self.volume_history = {item['code']: deque(maxlen=20) for item in item_list}

    def generate_signals(self, all_item_data_df): # 更改 all_etf_data_df 为更通用的 all_item_data_df
        results = []
        for item in self.item_list: 
            item_data_row = all_item_data_df[all_item_data_df['代码'] == item['code']]
            if not item_data_row.empty:
                current_data = item_data_row.iloc[0]
                self.volume_history[item['code']].append(current_data['成交额'])
                results.append(self._create_signal_dict(current_data, item))
        return results

    def _create_signal_dict(self, item_series, item_info):
        points = []
        code = item_series.get('代码')
        change = item_series.get('涨跌幅', 0)
        
        if change > 2.5: points.append("日内大幅上涨")
        if change < -2.5: points.append("日内大幅下跌")
        
        history = list(self.volume_history[code])
        if len(history) > 5:
            current_interval_volume = history[-1] - (history[-2] if len(history) > 1 else 0)
            avg_interval_volume = (history[-1] - history[0]) / (len(history) - 1) if len(history) > 1 else 0
            if avg_interval_volume > 0 and current_interval_volume > avg_interval_volume * 3:
                points.append("成交量异常放大")

        return {
            'code': code, 
            'name': item_info.get('name'), 
            'price': item_series.get('最新价'), 
            'change': change, 
            'analysis_points': points if points else ["盘中信号平稳"]
        }