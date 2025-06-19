import akshare as ak
import pandas as pd
from cachetools import cached, TTLCache
from dotenv import load_dotenv
import os
import logging
import asyncio
from tenacity import retry, stop_after_attempt, wait_fixed

logger = logging.getLogger(__name__)
load_dotenv() 

CACHE_EXPIRE = int(os.getenv('CACHE_EXPIRE_SECONDS', '60')) 
cache = TTLCache(maxsize=10, ttl=CACHE_EXPIRE)

# --- 您的核心ETF观察池 (您可以在这里自由增删) ---
CORE_ETF_POOL = [
    # ... (您的列表保持不变)
    {'code': '510050', 'name': '上证50ETF'},
    {'code': '510300', 'name': '沪深300ETF'},
    {'code': '510500', 'name': '中证500ETF'},
    {'code': '159919', 'name': '创业板50ETF'},
    {'code': '588000', 'name': '科创50ETF'},
    {'code': '512000', 'name': '券商ETF'},
    {'code': '159995', 'name': '芯片ETF'},
    {'code': '512690', 'name': '酒ETF'},
    {'code': '512010', 'name': '医药ETF'},
    {'code': '513050', 'name': '中概互联ETF'},
    {'code': '512800', 'name': '银行ETF'},
    {'code': '159992', 'name': '创新药ETF'},
    {'code': '515030', 'name': '新能源车ETF'},
    {'code': '159825', 'name': '农业ETF'},
    {'code': '518880', 'name': '黄金ETF'},
]
CORE_STOCK_POOL = [
    {'code': '603298', 'name': '杭叉集团'},
    {'code': '930901', 'name': '动漫游戏指数'},
    {'code': '000819', 'name': '有色金属'},
    {'code': '161129', 'name': '原油LOF易方达'},
    # 您可以在这里添加更多您关注的股票
]
# --- 核心数据获取与处理函数 (这部分保持不变) ---

@cached(cache)
def get_all_etf_spot_realtime():
    """获取所有ETF的实时行情数据 (带缓存)"""
    logger.info("正在从AKShare获取所有ETF实时数据...(缓存有效期: %s秒)", CACHE_EXPIRE)
    try:
        df = ak.fund_etf_spot_em()
        numeric_cols = ['最新价', '昨收', '成交额']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(subset=numeric_cols, inplace=True)
        # 计算涨跌幅
        df['涨跌幅'] = 0.0
        mask = df['昨收'] != 0
        df.loc[mask, '涨跌幅'] = ((df.loc[mask, '最新价'] - df.loc[mask, '昨收']) / df.loc[mask, '昨收']) * 100
        return df
    except Exception as e:
        logger.error(f"❌ 获取ETF实时数据失败: {e}", exc_info=True)
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get_etf_daily_history(etf_code: str):
    """获取单支ETF的历史日线数据 (带自动重试)"""
    logger.info(f"正在获取 {etf_code} 的历史日线数据...")
    try:
        daily_df = await asyncio.to_thread(
            ak.fund_etf_hist_em,
            symbol=etf_code,
            period="daily",
            adjust="qfq"
        )
        return daily_df
    except Exception as e:
        logger.warning(f"⚠️ 获取 {etf_code} 日线数据时出错 (将进行重试): {e}")
        raise e
@cached(cache)
def get_all_stock_spot_realtime():
    """获取所有A股的实时行情数据 (带缓存)"""
    logger.info("正在从AKShare获取所有A股实时数据...(缓存有效期: %s秒)", CACHE_EXPIRE)
    try:
        # 使用专门获取股票实时行情的接口
        df = ak.stock_zh_a_spot_em()
        numeric_cols = ['最新价', '昨收', '成交额']
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors='coerce')
        df.dropna(subset=numeric_cols, inplace=True)
        # 计算涨跌幅
        df['涨跌幅'] = 0.0
        mask = df['昨收'] != 0
        df.loc[mask, '涨跌幅'] = ((df.loc[mask, '最新价'] - df.loc[mask, '昨收']) / df.loc[mask, '昨收'])
        return df
    except Exception as e:
        logger.error(f"❌ 获取股票实时数据失败: {e}", exc_info=True)
        return None

@retry(stop=stop_after_attempt(3), wait=wait_fixed(2))
async def get_stock_daily_history(stock_code: str):
    """获取单支股票的历史日线数据 (带自动重试)"""
    logger.info(f"正在获取 {stock_code} 的历史日线数据...")
    try:
        # 使用专门获取股票历史数据的接口
        daily_df = await asyncio.to_thread(
            ak.stock_zh_a_hist,
            symbol=stock_code,
            period="daily",
            adjust="qfq"  # 使用前复权数据
        )
        return daily_df
    except Exception as e:
        logger.warning(f"⚠️ 获取 {stock_code} 日线数据时出错 (将进行重试): {e}")
        raise e