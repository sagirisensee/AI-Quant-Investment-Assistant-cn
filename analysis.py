import asyncio
import logging
import random
import pandas as pd
import pandas_ta as ta
from ak_utils import (
    get_all_etf_spot_realtime, get_etf_daily_history, CORE_ETF_POOL,
    get_all_stock_spot_realtime, get_stock_daily_history, CORE_STOCK_POOL
)
from llm_analyzer import get_llm_score_and_analysis
from indicators import analyze_ma, analyze_macd, analyze_bollinger
from indicators import judge_trend_status

logger = logging.getLogger(__name__)
pd.set_option('display.max_rows', None) 
pd.set_option('display.max_columns', None) 

async def generate_ai_driven_report(get_realtime_data_func, get_daily_history_func, core_pool):
    logger.info("启动AI驱动的统一全面分析引擎...")
    realtime_data_df_task = asyncio.to_thread(get_realtime_data_func)
    daily_trends_task = _get_daily_trends_generic(get_daily_history_func, core_pool)
    realtime_data_df, daily_trends_list = await asyncio.gather(realtime_data_df_task, daily_trends_task)
    if realtime_data_df is None:
        return [{"name": "错误", "code": "", "ai_score": 0, "ai_comment": "获取实时数据失败，无法分析。"}]
    daily_trends_map = {item['code']: item for item in daily_trends_list}
    if get_realtime_data_func == get_all_stock_spot_realtime:
        item_type = "stock"
    else:
        item_type = "etf"
    intraday_analyzer = _IntradaySignalGenerator(core_pool, item_type=item_type)
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
            if '收盘' in result.columns: 
                result.rename(columns={'收盘': 'close'}, inplace=True)
            elif 'close' not in result.columns and 'Close' in result.columns:
                result.rename(columns={'Close': 'close'}, inplace=True)

            if '最高' in result.columns:
                result.rename(columns={'最高': 'high'}, inplace=True)
            elif 'high' not in result.columns and 'High' in result.columns:
                result.rename(columns={'High': 'high'}, inplace=True)

            if '最低' in result.columns:
                result.rename(columns={'最低': 'low'}, inplace=True)
            elif 'low' not in result.columns and 'Low' in result.columns:
                result.rename(columns={'Low': 'low'}, inplace=True)

            if '日期' in result.columns:
                result['日期'] = pd.to_datetime(result['日期'])
                result.set_index('日期', inplace=True)
            elif 'date' in result.columns:
                result['date'] = pd.to_datetime(result['date'])
                result.set_index('date', inplace=True)
            result.index.name = None
            result['close'] = pd.to_numeric(result['close'], errors='coerce')
            if 'high' in result.columns:
                result['high'] = pd.to_numeric(result['high'], errors='coerce')
            if 'low' in result.columns:
                result['low'] = pd.to_numeric(result['low'], errors='coerce')
            if 'close' not in result.columns: # Removed 'high' and 'low' from this critical check
                analysis_report.append({**item_info, 'status': '🟡 数据列缺失', 'technical_indicators_summary': ["获取到的历史数据缺少必要的'close'列。"]})
                continue
            if len(result) < 60:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于60天)', 'technical_indicators_summary': ["历史数据不足60天，部分长期指标无法计算。"], 'raw_debug_data': {}})
                continue
            if result['close'].isnull().all():
                analysis_report.append({**item_info, 'status': '🟡 数据计算失败', 'technical_indicators_summary': ["'close' 列数据全为空值，无法计算指标。"]})
                continue

            result.ta.sma(close='close', length=5, append=True)
            result.ta.sma(close='close', length=10, append=True)
            result.ta.sma(close='close', length=20, append=True)
            result.ta.sma(close='close', length=60, append=True)
            result.ta.macd(close='close', append=True)
            result.ta.bbands(close='close', length=20, append=True)

            if len(result) < 2:
                analysis_report.append({**item_info, 'status': '🟡 数据不足 (少于2天)', 'technical_indicators_summary': ["历史数据不足2天，无法进行趋势分析。"], 'raw_debug_data': {}})
                continue
            latest = result.iloc[-1]
            prev_latest = result.iloc[-2]
            trend_signals = []

            analyze_ma(result, latest, prev_latest, trend_signals)
            analyze_macd(result, latest, prev_latest, trend_signals)
            analyze_bollinger(result, latest, prev_latest, trend_signals)


            # --- 状态判定 ---
            status = judge_trend_status(latest, prev_latest)
            analysis_report.append({
                **item_info,
                'status': status,
                'technical_indicators_summary': trend_signals,
                'raw_debug_data': {}
            })
        except Exception as e:
            logger.error(f"分析 {item_info.get('name', item_info['code'])} 时出错: {e}", exc_info=True)
            analysis_report.append({
                **item_info,
                'status': '❌ 分析失败',
                'technical_indicators_summary': [f"数据获取或分析过程中出现错误：{e}"],
                'raw_debug_data': {}
            })
    return analysis_report

class _IntradaySignalGenerator:
    def __init__(self, item_list, item_type):
        self.item_list = item_list
        self.item_type = item_type

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
        raw_change = item_series.get('涨跌幅', 0)
        if self.item_type == "stock":
            change = raw_change * 100
        else:
            change = raw_change
        if change > 2.5: points.append("日内大幅上涨")
        if change < -2.5: points.append("日内大幅下跌")
        return {
            'code': code,
            'name': item_info.get('name'),
            'price': item_series.get('最新价'),
            'change': change,
            'analysis_points': points if points else ["盘中信号平稳"]
        }


async def get_detailed_analysis_report_for_debug(get_realtime_data_func, get_daily_history_func, core_pool):
    logger.info("启动AI驱动的调试分析引擎，不调用LLM...")
    realtime_data_df_task = asyncio.to_thread(get_realtime_data_func)
    daily_trends_task = _get_daily_trends_generic(get_daily_history_func, core_pool)
    realtime_data_df, daily_trends_list = await asyncio.gather(realtime_data_df_task, daily_trends_task)
    if realtime_data_df is None:
        return [{"name": "错误", "code": "", "ai_comment": "获取实时数据失败，无法分析。"}]
    daily_trends_map = {item['code']: item for item in daily_trends_list}
    if get_realtime_data_func == get_all_stock_spot_realtime:
        item_type = "stock"
    else:
        item_type = "etf"
    intraday_analyzer = _IntradaySignalGenerator(core_pool, item_type=item_type)
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
