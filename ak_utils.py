import akshare as ak
import pandas as pd
from cachetools import cached, TTLCache
from dotenv import load_dotenv
import os
import logging
import asyncio
from tenacity import retry, stop_after_attempt, wait_fixed
import json 

logger = logging.getLogger(__name__)
load_dotenv() 

CACHE_EXPIRE = int(os.getenv('CACHE_EXPIRE_SECONDS', '60')) 
cache = TTLCache(maxsize=10, ttl=CACHE_EXPIRE)

def _load_pool_from_env(env_var_name: str, default_pool: list = None):
    """从环境变量加载JSON格式的观察池"""
    pool_json = os.getenv(env_var_name)
    if not pool_json:
        logger.warning(f" 环境变量 '{env_var_name}' 未设置，将使用默认或空列表。")
        return default_pool if default_pool is not None else []
    try:
        # 尝试解析JSON字符串
        return json.loads(pool_json)
    except json.JSONDecodeError:
        logger.error(f" 环境变量 '{env_var_name}' 中的JSON格式错误，无法解析。请检查是否所有字符串都使用了双引号且没有尾随逗号。将使用默认或空列表。")
        return default_pool if default_pool is not None else []
DEFAULT_ETF_POOL = [
    {'code': '510300', 'name': '沪深300ETF'},
    {'code': '159919', 'name': '创业板50ETF'}
]   
CORE_ETF_POOL = _load_pool_from_env('CORE_ETF_POOL_JSON', DEFAULT_ETF_POOL)
DEFAULT_STOCK_POOL = [
    {'code': '600519', 'name': '贵州茅台'}
]
CORE_STOCK_POOL = _load_pool_from_env('CORE_STOCK_POOL_JSON', DEFAULT_STOCK_POOL)

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
        logger.error(f" 获取ETF实时数据失败: {e}", exc_info=True)
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
        logger.error(f" 获取股票实时数据失败: {e}", exc_info=True)
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
        logger.warning(f" 获取 {stock_code} 日线数据时出错 (将进行重试): {e}")
        raise e