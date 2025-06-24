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
                analysis_report.append({**item_info, 'status': '🟡 数据不足', 'technical_indicators_summary': ["历史数据为空或无法获取。"]})
                continue
            if '收盘' not in result.columns and 'close' in result.columns:
                result.rename(columns={'close': '收盘'}, inplace=True)
            elif '收盘' not in result.columns and 'Close' in result.columns:
                result.rename(columns={'Close': '收盘'}, inplace=True)
            
            # 检查并统一 '成交量' 列名
            if '成交量' not in result.columns and 'volume' in result.columns:
                result.rename(columns={'volume': '成交量'}, inplace=True)
            elif '成交量' not in result.columns and 'Volume' in result.columns:
                result.rename(columns={'Volume': '成交量'}, inplace=True)
            
            # 确保 '日期' 列存在且是日期类型，并设置为索引 (如果akshare返回的不是)
            if '日期' in result.columns:
                result['日期'] = pd.to_datetime(result['日期'])
                result.set_index('日期', inplace=True)
            elif 'date' in result.columns:
                result['date'] = pd.to_datetime(result['date'])
                result.set_index('date', inplace=True)
            # 确保索引名称为None，避免pandas_ta问题
            result.index.name = None

            # 确保关键列现在存在
            if '收盘' not in result.columns or '成交量' not in result.columns:
                 analysis_report.append({**item_info, 'status': '🟡 数据列缺失', 'technical_indicators_summary': ["获取到的历史数据缺少必要的'收盘'或'成交量'列。"]})
                 continue            
            # 确保数据足够计算指标 (至少需要60条才能计算60日均线和60日成交量均线)
            if len(result) < 60:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于60天)', 'technical_indicators_summary': ["历史数据不足60天，部分长期指标无法计算。"]})
                continue

            # 计算所有需要的指标
            result.ta.sma(close='收盘', length=5, append=True)
            result.ta.sma(close='收盘', length=10, append=True)
            result.ta.sma(close='收盘', length=20, append=True)
            result.ta.sma(close='收盘', length=60, append=True)
            
            # 计算MACD
            # pandas_ta 生成的MACD列名通常是 MACD_12_26_9, MACDh_12_26_9, MACDs_12_26_9
            result.ta.macd(close='收盘', append=True) 

            # 计算60日成交量均线
            result.ta.sma(close='成交量', length=60, append=True, column_name='VOLUME_SMA_60')

            # 获取最新和倒数第二日的数据
            # 确保这些行索引存在
            if len(result) < 2:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于2天)', 'technical_indicators_summary': ["历史数据不足2天，无法进行趋势分析。"]})
                continue

            latest = result.iloc[-1]
            prev_latest = result.iloc[-2]

            trend_signals = [] # 用于存储本次分析提取的详细技术信号

            # ----------------------------------------------------
            # 均线整体排列状态 (判断趋势强度)
            # ----------------------------------------------------
            ma_cols = ['SMA_5', 'SMA_10', 'SMA_20', 'SMA_60']
            if all(col in latest and pd.notna(latest[col]) for col in ma_cols):
                is_bullish_stack = (latest['SMA_5'] > latest['SMA_10'] and
                                    latest['SMA_10'] > latest['SMA_20'] and
                                    latest['SMA_20'] > latest['SMA_60'])
                
                is_bearish_stack = (latest['SMA_5'] < latest['SMA_10'] and
                                    latest['SMA_10'] < latest['SMA_20'] and
                                    latest['SMA_20'] < latest['SMA_60'])

                if is_bullish_stack:
                    trend_signals.append("均线呈强势多头排列 (5 > 10 > 20 > 60日线)，趋势强劲。")
                elif is_bearish_stack:
                    trend_signals.append("均线呈弱势空头排列 (5 < 10 < 20 < 60日线)，趋势疲弱。")
                else:
                    trend_signals.append("均线排列纠缠 (处于震荡或趋势转换期)。")
            else:
                trend_signals.append("部分均线数据缺失，无法判断均线排列状态。") # 确保有描述

            # ----------------------------------------------------
            # 1. 移动平均线（MA）关系
            # ----------------------------------------------------
            # 股价与均线关系 - 确保高于和低于都有信号
            for length in [5, 10, 20, 60]:
                sma_col = f'SMA_{length}'
                if sma_col in latest and pd.notna(latest[sma_col]):
                    if latest['收盘'] > latest[sma_col]:
                        trend_signals.append(f"股价高于{length}日均线。")
                    else:
                        trend_signals.append(f"股价低于{length}日均线。")
                else:
                    trend_signals.append(f"{length}日均线数据缺失，无法判断股价与均线关系。") # 确保有描述

            # 均线交叉（金叉/死叉） - 确保金叉、死叉和排列都有信号
            ma_pairs = [(5, 10), (10, 20), (20, 60)]
            for s_len, l_len in ma_pairs:
                s_col = f'SMA_{s_len}'
                l_col = f'SMA_{l_len}'
                if all(col in latest and col in prev_latest and pd.notna(latest[col]) and pd.notna(prev_latest[col]) for col in [s_col, l_col]):
                    if latest[s_col] > latest[l_col] and prev_latest[s_col] <= prev_latest[l_col]:
                        trend_signals.append(f"{s_len}日均线金叉{l_len}日均线 (看涨信号)。")
                    elif latest[s_col] < latest[l_col] and prev_latest[s_col] >= prev_latest[l_col]:
                        trend_signals.append(f"{s_len}日均线死叉{l_len}日均线 (看跌信号)。")
                    else:
                        if latest[s_col] > latest[l_col]:
                            trend_signals.append(f"{s_len}日均线在{l_len}日均线上方，多头排列延续。")
                        else:
                            trend_signals.append(f"{s_len}日均线在{l_len}日均线下方，空头排列延续。")
                else:
                    trend_signals.append(f"{s_len}日与{l_len}日均线数据缺失，无法判断交叉与排列。") # 确保有描述

            # 60日均线趋势 - 确保向上、向下和持平都有信号
            if 'SMA_60' in latest and 'SMA_60' in prev_latest and pd.notna(latest['SMA_60']) and pd.notna(prev_latest['SMA_60']):
                if latest['SMA_60'] > prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向上 (中长期趋势积极)。")
                elif latest['SMA_60'] < prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向下 (中长期趋势谨慎)。")
                else:
                    trend_signals.append("60日均线趋势持平 (中长期趋势中性)。")
            else:
                trend_signals.append("60日均线数据缺失，无法判断其趋势方向。") # 确保有描述

            # ----------------------------------------------------
            # 2. MACD 指标 - 确保所有状态都有描述，包括数据缺失情况
            # ----------------------------------------------------
            macd_line_col = 'MACD_12_26_9'
            signal_line_col = 'MACDs_12_26_9'
            histogram_col = 'MACDh_12_26_9'

            # 整体检查MACD相关列是否存在且非空
            if all(col in latest and pd.notna(latest[col]) for col in [macd_line_col, signal_line_col, histogram_col]) and \
               all(col in prev_latest and pd.notna(prev_latest[col]) for col in [macd_line_col, signal_line_col, histogram_col]):

                # MACD金叉/死叉信号
                if latest[macd_line_col] > latest[signal_line_col] and prev_latest[macd_line_col] <= prev_latest[signal_line_col]:
                    trend_signals.append("MACD金叉 (看涨信号)。")
                elif latest[macd_line_col] < latest[signal_line_col] and prev_latest[macd_line_col] >= prev_latest[signal_line_col]:
                    trend_signals.append("MACD死叉 (看跌信号)。")
                else:
                    if latest[macd_line_col] > latest[signal_line_col]:
                        trend_signals.append("MACD线在信号线上方 (多头延续)。")
                    else:
                        trend_signals.append("MACD线在信号线下方 (空头延续)。")
                
                # MACD线与零轴关系 - 确保正负都有描述
                if latest[macd_line_col] > 0:
                    trend_signals.append("MACD线在零轴上方 (强势区域)。")
                elif latest[macd_line_col] < 0:
                    trend_signals.append("MACD线在零轴下方 (弱势区域)。")
                else:
                    trend_signals.append("MACD线在零轴附近 (中性区域)。")
                
                # MACD柱线变化 - 确保所有变化都有描述
                if latest[histogram_col] > 0:
                    if latest[histogram_col] > prev_latest[histogram_col]:
                        trend_signals.append("MACD红柱增长 (多头力量增强)。")
                    elif latest[histogram_col] < prev_latest[histogram_col]:
                        trend_signals.append("MACD红柱缩短 (多头力量减弱)。")
                    else:
                        trend_signals.append("MACD红柱持平 (多头力量维持)。")
                elif latest[histogram_col] < 0:
                    if latest[histogram_col] < prev_latest[histogram_col]:
                        trend_signals.append("MACD绿柱增长 (空头力量增强)。")
                    elif latest[histogram_col] > prev_latest[histogram_col]:
                        trend_signals.append("MACD绿柱缩短 (空头力量减弱)。")
                    else:
                        trend_signals.append("MACD绿柱持平 (空头力量维持)。")
                else:
                    trend_signals.append("MACD柱线在零轴 (多空平衡)。")
            else:
                trend_signals.append("MACD指标数据缺失或不完整，无法分析。") # 确保有描述
            
            # ----------------------------------------------------
            # 3. 60日历史成交量信号 - 确保正常、放大、萎缩都有描述
            # ----------------------------------------------------
            volume_col = '成交量'
            volume_sma_col = 'VOLUME_SMA_60'

            if volume_col in latest and volume_sma_col in latest and \
               pd.notna(latest[volume_col]) and pd.notna(latest[volume_sma_col]) and latest[volume_sma_col] > 0:
                
                volume_ratio = latest[volume_col] / latest[volume_sma_col]
                if volume_ratio > 2:
                    trend_signals.append("成交量较60日均量显著放大 (量能异常活跃)。") # 更强烈的描述
                elif volume_ratio < 0.5:
                    trend_signals.append("成交量较60日均量显著萎缩 (量能极度清淡)。") # 更强烈的描述
                elif volume_ratio >= 1.5: # 调整阈值，更细致地分级
                    trend_signals.append("成交量较60日均量放大 (量能活跃)。")
                elif volume_ratio <= 0.75: # 调整阈值
                    trend_signals.append("成交量较60日均量萎缩 (量能清淡)。")
                else:
                    trend_signals.append("成交量接近60日均量 (量能正常)。")
            else:
                trend_signals.append("成交量数据缺失，无法分析量能情况。") # 确保有描述

            # ----------------------------------------------------
            # 最终报告状态
            # ----------------------------------------------------
            # 这里可以保持基于20日均线的判断，或者更复杂一些
            status = '🟢 上升趋势' if 'SMA_20' in latest and pd.notna(latest['SMA_20']) and latest['收盘'] > latest['SMA_20'] else '🔴 下降趋势'
            if status == '🟢 上升趋势' and 'SMA_20' in latest and 'SMA_60' in latest and pd.notna(latest['SMA_20']) and pd.notna(latest['SMA_60']) and latest['SMA_20'] > latest['SMA_60']:
                 status = '🟢 强势上升趋势'
            elif status == '🔴 下降趋势' and 'SMA_20' in latest and 'SMA_60' in latest and pd.notna(latest['SMA_20']) and pd.notna(latest['SMA_60']) and latest['SMA_20'] < latest['SMA_60']:
                 status = '🔴 弱势下降趋势'
            else:
                 status = '🟡 震荡趋势' # 增加一个震荡或盘整的描述

            analysis_report.append({
                **item_info,
                'status': status, # 传递更详细的整体趋势描述
                'technical_indicators_summary': trend_signals
            })
        except Exception as e:
            logger.error(f"❌ 分析 {item_info.get('name', item_info['code'])} 日线数据时失败: {e}", exc_info=True)
            analysis_report.append({**item_info, 'status': '❌ 分析失败', 'technical_indicators_summary': [f"数据获取或分析过程中出现错误：{e}"]}) # 确保错误信息也传递给LLM
        await asyncio.sleep(random.uniform(1.0, 2.0)) # 保持请求间隔
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
"""
async def get_detailed_analysis_report_for_debug(get_realtime_data_func, get_daily_history_func, core_pool):
   
    "生成详细的量化分析报告，不调用LLM，主要用于调试。参数同 generate_ai_driven_report。"
    logger.info("启动AI驱动的调试分析引擎，不调用LLM...")
    
    # 并行获取实时数据和日线趋势
    realtime_data_df_task = asyncio.to_thread(get_realtime_data_func)
    daily_trends_task = _get_daily_trends_generic(get_daily_history_func, core_pool)
    
    realtime_data_df, daily_trends_list = await asyncio.gather(realtime_data_df_task, daily_trends_task)
    
    if realtime_data_df is None:
        return [{"name": "错误", "code": "", "ai_comment": "获取实时数据失败，无法分析。"}]
    
    daily_trends_map = {item['code']: item for item in daily_trends_list}
    
    # 复用盘中信号生成器，传入当前使用的观察池
    intraday_analyzer = _IntradaySignalGenerator(core_pool)
    intraday_signals = intraday_analyzer.generate_signals(realtime_data_df)
    
    debug_report = []
    for i, signal in enumerate(intraday_signals):
        code = signal['code']
        name = signal['name']
        logger.info(f"正在准备调试报告: {name} ({i+1}/{len(intraday_signals)})")
        
        daily_trend_info = daily_trends_map.get(code, {'status': '未知', 'technical_indicators_summary': []})
        
        debug_report.append({
            'code': code,
            'name': name,
            'price': signal.get('price'),
            'change': signal.get('change'),
            'intraday_signals': signal.get('analysis_points'),
            'daily_trend_status': daily_trend_info.get('status'),
            'technical_indicators_summary': daily_trend_info.get('technical_indicators_summary')
        })
        # 保持礼貌的请求间隔，即使不调用LLM也建议有间隔
        await asyncio.sleep(random.uniform(0.5, 1.0)) # 可以缩短延迟，因为不调用LLM

    return debug_report
"""