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
    intraday_analyzer = _IntradaySignalGenerator(core_pool)
    intraday_signals = intraday_analyzer.generate_signals(realtime_data_df)
    final_report = []
    for i, signal in enumerate(intraday_signals):
        code = signal['code']
        name = signal['name']
        logger.info(f"正在调用LLM分析: {name} ({i+1}/{len(intraday_signals)})")
        try:
            daily_trend = daily_trends_map.get(code, {'status': '未知'})
            ai_score, ai_comment = await get_llm_score_and_analysis(signal, daily_trend)
            final_report.append({
                **signal,
                "ai_score": ai_score if ai_score is not None else 0,
                "ai_comment": ai_comment
            })
        except Exception as e:
            logger.error(f"处理LLM分析 {name} 时发生错误: {e}")
            final_report.append({**signal, "ai_score": 0, "ai_comment": "处理时发生未知错误。"})
        await asyncio.sleep(random.uniform(1.0, 2.5))
    return sorted(final_report, key=lambda x: x.get('ai_score', 0), reverse=True)

async def _get_daily_trends_generic(get_daily_history_func, core_pool):
    analysis_report = []
    for item_info in core_pool:
        try:
            result = await get_daily_history_func(item_info['code'])
            if result is None or result.empty:
                analysis_report.append({**item_info, 'status': '🟡 数据不足', 'technical_indicators_summary': ["历史数据为空或无法获取。"], 'raw_debug_data': {}})
                continue
            # 字段标准化
            if '收盘' not in result.columns and 'close' in result.columns:
                result.rename(columns={'close': '收盘'}, inplace=True)
            elif '收盘' not in result.columns and 'Close' in result.columns:
                result.rename(columns={'Close': '收盘'}, inplace=True)
            if '日期' in result.columns:
                result['日期'] = pd.to_datetime(result['日期'])
                result.set_index('日期', inplace=True)
            elif 'date' in result.columns:
                result['date'] = pd.to_datetime(result['date'])
                result.set_index('date', inplace=True)
            result.index.name = None
            result['收盘'] = pd.to_numeric(result['收盘'], errors='coerce')
            # 检查关键列
            if '收盘' not in result.columns:
                analysis_report.append({**item_info, 'status': '🟡 数据列缺失', 'technical_indicators_summary': ["获取到的历史数据缺少必要的'收盘'列。"]})
                continue
            if len(result) < 60:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于60天)', 'technical_indicators_summary': ["历史数据不足60天，部分长期指标无法计算。"], 'raw_debug_data': {}})
                continue
            # 价格均线
            result.ta.sma(close='收盘', length=5, append=True)
            result.ta.sma(close='收盘', length=10, append=True)
            result.ta.sma(close='收盘', length=20, append=True)
            result.ta.sma(close='收盘', length=60, append=True)
            # MACD
            result.ta.macd(close='收盘', append=True)
            if len(result) < 2:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于2天)', 'technical_indicators_summary': ["历史数据不足2天，无法进行趋势分析。"], 'raw_debug_data': {}})
                continue
            latest = result.iloc[-1]
            prev_latest = result.iloc[-2]
            trend_signals = []
            ma_debug = {}
            for length in [5, 10, 20, 60]:
                col = f'SMA_{length}'
                val = latest.get(col, None)
                ma_debug[col] = val if pd.notna(val) else None
            raw_debug_data = {
                '收盘': latest.get('收盘', None),
                **ma_debug
            }
            for length in [5, 10, 20, 60]:
                col = f'SMA_{length}'
                prev_val = prev_latest.get(col, None)
                raw_debug_data[f"{col}_prev"] = prev_val if pd.notna(prev_val) else None
            ma_values = [f"{col}: {val:.3f}" if val is not None else f"{col}: 缺失" for col, val in ma_debug.items()]
            trend_signals.append("【均线最新数值】" + " | ".join(ma_values))
            trend_signals.append(f"收盘价: {raw_debug_data['收盘']}")
            # 均线排列
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
                trend_signals.append("部分均线数据缺失，无法判断均线排列状态。")
            # 股价与均线关系
            for length in [5, 10, 20, 60]:
                sma_col = f'SMA_{length}'
                if sma_col in latest and pd.notna(latest[sma_col]):
                    if latest['收盘'] > latest[sma_col]:
                        trend_signals.append(f"股价高于{length}日均线。")
                    else:
                        trend_signals.append(f"股价低于{length}日均线。")
                else:
                    trend_signals.append(f"{length}日均线数据缺失，无法判断股价与均线关系。")
            # 均线交叉（金叉/死叉）
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
                    trend_signals.append(f"{s_len}日与{l_len}日均线数据缺失，无法判断交叉与排列。")
            # 60日均线趋势
            if 'SMA_60' in latest and 'SMA_60' in prev_latest and pd.notna(latest['SMA_60']) and pd.notna(prev_latest['SMA_60']):
                if latest['SMA_60'] > prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向上 (中长期趋势积极)。")
                elif latest['SMA_60'] < prev_latest['SMA_60']:
                    trend_signals.append("60日均线趋势向下 (中长期趋势谨慎)。")
                else:
                    trend_signals.append("60日均线趋势持平 (中长期趋势中性)。")
            else:
                trend_signals.append("60日均线数据缺失，无法判断其趋势方向。")
            # MACD指标
            macd_line_col = 'MACD_12_26_9'
            signal_line_col = 'MACDs_12_26_9'
            histogram_col = 'MACDh_12_26_9'
            if all(col in latest and pd.notna(latest[col]) for col in [macd_line_col, signal_line_col, histogram_col]) and \
               all(col in prev_latest and pd.notna(prev_latest[col]) for col in [macd_line_col, signal_line_col, histogram_col]):
                if latest[macd_line_col] > latest[signal_line_col] and prev_latest[macd_line_col] <= prev_latest[signal_line_col]:
                    trend_signals.append("MACD金叉 (看涨信号)。")
                elif latest[macd_line_col] < latest[signal_line_col] and prev_latest[macd_line_col] >= prev_latest[signal_line_col]:
                    trend_signals.append("MACD死叉 (看跌信号)。")
                else:
                    if latest[macd_line_col] > latest[signal_line_col]:
                        trend_signals.append("MACD线在信号线上方 (多头延续)。")
                    else:
                        trend_signals.append("MACD线在信号线下方 (空头延续)。")
                if latest[macd_line_col] > 0:
                    trend_signals.append("MACD线在零轴上方 (强势区域)。")
                elif latest[macd_line_col] < 0:
                    trend_signals.append("MACD线在零轴下方 (弱势区域)。")
                else:
                    trend_signals.append("MACD线在零轴附近 (中性区域)。")
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
                trend_signals.append("MACD指标数据缺失或不完整，无法分析。")
            # 最终报告状态
            status = '🟢 上升趋势' if 'SMA_20' in latest and pd.notna(latest['SMA_20']) and latest['收盘'] > latest['SMA_20'] else '🔴 下降趋势'
            if status == '🟢 上升趋势' and 'SMA_20' in latest and 'SMA_60' in latest and pd.notna(latest['SMA_20']) and pd.notna(latest['SMA_60']) and latest['SMA_20'] > latest['SMA_60']:
                status = '🟢 强势上升趋势'
            elif status == '🔴 下降趋势' and 'SMA_20' in latest and 'SMA_60' in latest and pd.notna(latest['SMA_20']) and pd.notna(latest['SMA_60']) and latest['SMA_20'] < latest['SMA_60']:
                status = '🔴 弱势下降趋势'
            else:
                status = '🟡 震荡趋势'
            analysis_report.append({
                **item_info,
                'status': status,
                'technical_indicators_summary': trend_signals,
                'raw_debug_data': raw_debug_data
            })
        except Exception as e:
            logger.error(f"❌ 分析 {item_info.get('name', item_info['code'])} 日线数据时失败: {e}", exc_info=True)
            analysis_report.append({**item_info, 'status': '❌ 分析失败', 'technical_indicators_summary': [f"数据获取或分析过程中出现错误：{e}"], 'raw_debug_data': {}})
        await asyncio.sleep(random.uniform(1.0, 2.0))
    return analysis_report


class _IntradaySignalGenerator:
    """内部辅助类：生成盘中量化信号（只看价格涨跌幅）"""
    def __init__(self, item_list):
        self.item_list = item_list

    def generate_signals(self, all_item_data_df):
        results = []
        for item in self.item_list:
            item_data_row = all_item_data_df[all_item_data_df['代码'] == item['code']]
            if not item_data_row.empty:
                current_data = item_data_row.iloc[0]
                results.append(self._create_signal_dict(current_data, item))
        return results

    def _create_signal_dict(self, item_series, item_info):
        points = []
        code = item_series.get('代码')
        change = item_series.get('涨跌幅', 0)
        if change > 2.5: points.append("日内大幅上涨")
        if change < -2.5: points.append("日内大幅下跌")
        return {
            'code': code,
            'name': item_info.get('name'),
            'price': item_series.get('最新价'),
            'change': change,
            'analysis_points': points if points else ["盘中信号平稳"]
        }
'''
async def get_detailed_analysis_report_for_debug(get_realtime_data_func, get_daily_history_func, core_pool):
    logger.info("启动AI驱动的调试分析引擎，不调用LLM...")
    realtime_data_df_task = asyncio.to_thread(get_realtime_data_func)
    daily_trends_task = _get_daily_trends_generic(get_daily_history_func, core_pool)
    realtime_data_df, daily_trends_list = await asyncio.gather(realtime_data_df_task, daily_trends_task)
    if realtime_data_df is None:
        return [{"name": "错误", "code": "", "ai_comment": "获取实时数据失败，无法分析。"}]
    daily_trends_map = {item['code']: item for item in daily_trends_list}
    intraday_analyzer = _IntradaySignalGenerator(core_pool)
    intraday_signals = intraday_analyzer.generate_signals(realtime_data_df)
    debug_report = []
    for i, signal in enumerate(intraday_signals):
        code = signal['code']
        name = signal['name']
        logger.info(f"正在准备调试报告: {name} ({i+1}/{len(intraday_signals)})")
        daily_trend_info = daily_trends_map.get(code, {'status': '未知', 'technical_indicators_summary': [], 'raw_debug_data': {}})
        raw_debug_data = daily_trend_info.get('raw_debug_data', {})
        if not raw_debug_data:
            raw_debug_data = {}
        debug_report.append({
            'code': code,
            'name': name,
            'price': signal.get('price'),
            'change': signal.get('change'),
            'intraday_signals': signal.get('analysis_points'),
            'daily_trend_status': daily_trend_info.get('status'),
            'technical_indicators_summary': daily_trend_info.get('technical_indicators_summary'),
            'raw_debug_data': raw_debug_data
        })
        await asyncio.sleep(random.uniform(0.5, 1.0))
    return debug_report
    '''