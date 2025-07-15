import pandas as pd

def analyze_bollinger(result, latest, prev_latest, trend_signals):
    try:
        upper = latest.get('BBU_20_2.0')
        middle = latest.get('BBM_20_2.0')
        lower = latest.get('BBL_20_2.0')
        close = latest.get('close') # Corrected: using 'close'
        
        # Ensure all necessary Bollinger Band values and close price are not NaN
        if pd.notna(upper) and pd.notna(middle) and pd.notna(lower) and pd.notna(close): 
            if close > upper:
                trend_signals.append("收盘价突破布林上轨，短线超买，警惕回调。")
            elif close < lower:
                trend_signals.append("收盘价跌破布林下轨，短线超卖，关注反弹。")
            elif close > middle:
                trend_signals.append("收盘价位于布林中轨之上，趋势偏强。")
            elif close < middle:
                trend_signals.append("收盘价位于布林中轨之下，趋势偏弱。")
            
            # 震荡判别：检查最近几个交易日穿越布林中轨的次数
            # 修正：避免直接比较 Series 对象，而是检查每日穿越情况
            # 需要至少两个数据点来判断交叉
            if len(result) >= 2 and 'close' in result.columns and 'BBM_20_2.0' in result.columns:
                cross_count = 0
                # 检查最近 N 天的穿越，例如最近 5 个交易日
                # 需要至少 6 个数据点才能检查到 5 次"前一天"和"当前天"
                # 更稳健的做法是获取最近的完整数据段
                num_days_to_check = min(len(result), 5) # Check up to the last 5 days available
                
                # Iterate from the second-to-last day back to check for crosses
                for i in range(1, num_days_to_check):
                    current_idx = -1 - (num_days_to_check - 1 - i) # Current day index (from tail)
                    prev_idx = current_idx - 1 # Previous day index (from tail)

                    current_close = result['close'].iloc[current_idx]
                    current_middle = result['BBM_20_2.0'].iloc[current_idx]
                    prev_close = result['close'].iloc[prev_idx]
                    prev_middle = result['BBM_20_2.0'].iloc[prev_idx]

                    if pd.notna(current_close) and pd.notna(current_middle) and \
                       pd.notna(prev_close) and pd.notna(prev_middle):
                        
                        # A cross occurs if the relationship (close > middle) changes from previous day to current day
                        # i.e., (prev_close <= prev_middle AND current_close > current_middle) OR
                        #       (prev_close >= prev_middle AND current_close < current_middle)
                        if (prev_close <= prev_middle and current_close > current_middle) or \
                           (prev_close >= prev_middle and current_close < current_middle):
                            cross_count += 1
                
                # If there are frequent crosses (e.g., 2 or more in 5 days), it indicates oscillation
                if cross_count >= 2: # Lowering threshold slightly as 5 days is a short window
                    trend_signals.append("近期收盘价频繁上下穿布林中轨，市场震荡明显。")
            else:
                trend_signals.append("布林通道震荡判别数据不足或列缺失。") 
        else:
            trend_signals.append("布林通道数据不足或关键数据缺失，无法分析。")
    except Exception as e: 
        trend_signals.append(f"布林通道分析异常：{e}，跳过分析。")


def judge_trend_status(latest, prev_latest):
    """
    综合均线、布林通道等，返回趋势状态字符串。
    """
    status = '🟡 震荡趋势' # Default to neutral/sideways
    
    close = latest.get('close') # Using 'close'
    if pd.isna(close):
        return '🟡 数据异常' 

    sma_20 = latest.get('SMA_20')
    sma_60 = latest.get('SMA_60')

    # Primary trend based on close vs SMA_20
    if pd.notna(sma_20):
        if close > sma_20:
            status = '🟢 上升趋势'
        else:
            status = '🔴 下降趋势'
    else:
        status = '🟡 均线数据不足' 

    # Refine trend based on SMA_20 vs SMA_60
    if pd.notna(sma_20) and pd.notna(sma_60):
        # Check for consistent uptrend
        if close > sma_20 and sma_20 > sma_60:
            status = '🟢 强势上升趋势'
        # Check for consistent downtrend
        elif close < sma_20 and sma_20 < sma_60:
            status = '🔴 弱势下降趋势'
        else:
            status = '🟡 震荡趋势'
    else:
        # If major SMAs are missing, retain previous status or default to neutral
        if status not in ['🟡 均线数据不足']: # Don't overwrite if it's already '数据不足'
            status = '🟡 震荡趋势' 

    # Bollinger Bands for confirming oscillation
    middle_bb = latest.get('BBM_20_2.0')
    if pd.notna(middle_bb) and pd.notna(close): # Ensure close is also available for this check
        # If close price is very near the middle band, it suggests oscillation
        if abs(close - middle_bb) / middle_bb < 0.005: 
            status = '🟡 震荡趋势'

    return status

def analyze_ma(result, latest, prev_latest, trend_signals):
    """
    均线（MA）信号分析
    """
    try:
        close = latest.get('close') # Using 'close'
        if pd.isna(close):
            trend_signals.append("收盘价数据缺失，无法进行均线分析。")
            return

        # 股价与均线关系
        for length in [5, 10, 20, 60]:
            col = f'SMA_{length}'
            val = latest.get(col)
            if pd.notna(val):
                if close > val: 
                    trend_signals.append(f"股价高于{length}日均线。")
                else:
                    trend_signals.append(f"股价低于{length}日均线。")
            else:
                trend_signals.append(f"{length}日均线数据缺失。")

        # 均线交叉（金叉/死叉）
        ma_pairs = [(5, 10), (10, 20), (20, 60)]
        for s_len, l_len in ma_pairs:
            s_col = f'SMA_{s_len}'
            l_col = f'SMA_{l_len}'
            
            # 修正：直接使用布尔表达式，避免再次包装在 all() 中
            # 检查当前和前一日均线值是否都可用
            current_s_val = latest.get(s_col)
            current_l_val = latest.get(l_col)
            prev_s_val = prev_latest.get(s_col)
            prev_l_val = prev_latest.get(l_col)

            # --- 修正后的条件判断 ---
            if pd.notna(current_s_val) and pd.notna(current_l_val) and \
               pd.notna(prev_s_val) and pd.notna(prev_l_val):
                
                # Check for Golden Cross
                if current_s_val > current_l_val and prev_s_val <= prev_l_val:
                    trend_signals.append(f"{s_len}日均线金叉{l_len}日均线（看涨信号）。")
                # Check for Death Cross
                elif current_s_val < current_l_val and prev_s_val >= prev_l_val:
                    trend_signals.append(f"{s_len}日均线死叉{l_len}日均线（看跌信号）。")
                else:
                    # Current arrangement description if no cross
                    if current_s_val > current_l_val:
                        trend_signals.append(f"{s_len}日均线在{l_len}日均线上方，多头排列延续。")
                    else:
                        trend_signals.append(f"{s_len}日均线在{l_len}日均线下方，空头排列延续。")
            else:
                trend_signals.append(f"{s_len}日与{l_len}日均线数据缺失，无法判断交叉。")
        
        # 60日均线趋势 (Long-term trend)
        sma_60_latest = latest.get('SMA_60')
        sma_60_prev = prev_latest.get('SMA_60')

        if pd.notna(sma_60_latest) and pd.notna(sma_60_prev):
            if sma_60_latest > sma_60_prev:
                trend_signals.append("60日均线趋势向上（中长期趋势积极）。")
            elif sma_60_latest < sma_60_prev:
                trend_signals.append("60日均线趋势向下（中长期趋势谨慎）。")
            else:
                trend_signals.append("60日均线趋势持平（中长期趋势中性）。")
        else:
            trend_signals.append("60日均线数据缺失，无法判断趋势。")
    except Exception as e: 
        trend_signals.append(f"均线分析异常：{e}，跳过分析。")

def analyze_macd(result, latest, prev_latest, trend_signals):
    """
    MACD信号分析
    """
    try:
        macd_line_col = 'MACD_12_26_9'
        signal_line_col = 'MACDs_12_26_9'
        histogram_col = 'MACDh_12_26_9'
        
        l_macd = latest.get(macd_line_col)
        l_signal = latest.get(signal_line_col)
        l_hist = latest.get(histogram_col)
        p_macd = prev_latest.get(macd_line_col)
        p_signal = prev_latest.get(signal_line_col)
        p_hist = prev_latest.get(histogram_col)
        
        # Check if all necessary MACD values are not NaN before proceeding
        if all(pd.notna(x) for x in [l_macd, l_signal, l_hist, p_macd, p_signal, p_hist]):
            # 金叉/死叉
            if l_macd > l_signal and p_macd <= p_signal:
                trend_signals.append("MACD金叉（看涨信号）。")
            elif l_macd < l_signal and p_macd >= p_signal:
                trend_signals.append("MACD死叉（看跌信号）。")
            else:
                if l_macd > l_signal:
                    trend_signals.append("MACD线在信号线上方，多头延续。")
                else:
                    trend_signals.append("MACD线在信号线下方，空头延续。")
            
            # 零轴
            if l_macd > 0:
                trend_signals.append("MACD线在零轴上方，市场偏强。")
            elif l_macd < 0:
                trend_signals.append("MACD线在零轴下方，市场偏弱。")
            else:
                trend_signals.append("MACD线在零轴附近，市场中性。")
            
            # 柱线变化
            if l_hist > 0: # Red bars (positive histogram)
                if l_hist > p_hist:
                    trend_signals.append("MACD红柱增长，多头力量增强。")
                elif l_hist < p_hist:
                    trend_signals.append("MACD红柱缩短，多头力量减弱。")
                else:
                    trend_signals.append("MACD红柱持平，多头力量维持。")
            elif l_hist < 0: # Green bars (negative histogram)
                if l_hist < p_hist: # Histogram becomes more negative
                    trend_signals.append("MACD绿柱增长，空头力量增强。")
                elif l_hist > p_hist: # Histogram becomes less negative (moves towards zero)
                    trend_signals.append("MACD绿柱缩短，空头力量减弱。")
                else:
                    trend_signals.append("MACD绿柱持平，空头力量维持。")
            else: # Histogram is zero
                trend_signals.append("MACD柱线在零轴，多空平衡。")
        else:
            trend_signals.append("MACD指标数据缺失或不完整，无法分析。")
    except Exception as e: 
        trend_signals.append(f"MACD分析异常：{e}，跳过分析。")