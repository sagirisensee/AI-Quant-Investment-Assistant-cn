import asyncio
import logging
import random
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
    获取指定观察池中所有项目的日线趋势。
    参数:
        get_daily_history_func: 获取历史日线数据的函数
        core_pool: 核心观察池
    """
    analysis_report = []
    for item_info in core_pool:
        try:
            # 调用传入的历史数据函数
            result = await get_daily_history_func(item_info['code'])
            if result is None or result.empty:
                analysis_report.append({**item_info, 'status': '🟡 数据不足'})
                continue
            
            # 注意：akshare返回的列名是中文
            result.ta.sma(close='收盘', length=20, append=True)
            latest = result.iloc[-1]
            status = '🟢 上升趋势' if latest['收盘'] > latest['SMA_20'] else '🔴 下降趋势'
            analysis_report.append({**item_info, 'status': status})
        except Exception:
            analysis_report.append({**item_info, 'status': '❌ 分析失败'})
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