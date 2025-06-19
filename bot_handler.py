from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
import logging
# 导入新的统一分析函数
from analysis import generate_ai_driven_report
# 导入 ak_utils 中的数据获取函数和观察池
from ak_utils import (
    get_all_etf_spot_realtime, get_etf_daily_history, CORE_ETF_POOL,
    get_all_stock_spot_realtime, get_stock_daily_history, CORE_STOCK_POOL
)


logger = logging.getLogger(__name__)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """欢迎信息"""
    welcome_text = (
        "🚀 **AI驱动的ETF/股票分析机器人**\n" # 更新欢迎语
        "--------------------------\n"
        "我将为您核心观察池中的所有ETF和股票提供由大语言模型生成的综合评分和交易点评。\n\n"
        "**📌 可用命令:**\n"
        "/analyze - 开始全面AI分析ETF\n" # 更新命令说明
        "/analyze_stocks - 开始全面AI分析股票\n" # 更新命令说明
        "/help - 显示此帮助信息"
    )
    await update.message.reply_text(welcome_text, parse_mode='Markdown')


async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行全面的ETF AI分析"""
    logger.info("收到 /analyze 命令，启动ETF AI分析...")
    await update.message.reply_text("好的，正在为您启动ETF分析引擎... \n这需要为每支ETF调用大模型，过程可能需要1-2分钟，请耐心等待最终报告。")
    
    # 调用统一分析函数，传入ETF相关参数
    report_data = await generate_ai_driven_report(
        get_realtime_data_func=get_all_etf_spot_realtime,
        get_daily_history_func=get_etf_daily_history,
        core_pool=CORE_ETF_POOL
    )
    
    if not report_data:
        await update.message.reply_text("未能生成ETF AI分析报告，请稍后再试。")
        return


    message = "🤖 **核心ETF池AI分析报告**\n(按AI综合评分排序)\n--------------------------\n\n"
    for i, item in enumerate(report_data, 1):
        message += (
            f"🏅 #{i} **{item.get('name')} ({item.get('code')})**\n"
            f"  - AI评分: **{item.get('ai_score', 'N/A')} / 100**\n"
            f"  - AI点评: *{item.get('ai_comment', '无')}*\n\n"
        )
    
    # 防止消息过长，进行分段发送
    if len(message) > 4096:
        await update.message.reply_text("报告过长，将分段发送...")
        for i in range(0, len(message), 4096):
            await update.message.reply_text(message[i:i+4096], parse_mode='Markdown')
    else:
        await update.message.reply_text(message, parse_mode='Markdown')


async def analyze_stocks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行全面的股票AI分析"""
    logger.info("收到 /analyze_stocks 命令，启动股票AI分析...")
    await update.message.reply_text("好的，正在为您启动股票分析引擎...\n这可能需要1-2分钟，请耐心等待最终报告。")
    
    # 调用统一分析函数，传入股票相关参数
    report_data = await generate_ai_driven_report(
        get_realtime_data_func=get_all_stock_spot_realtime,
        get_daily_history_func=get_stock_daily_history,
        core_pool=CORE_STOCK_POOL
    )
    
    if not report_data:
        await update.message.reply_text("未能生成股票AI分析报告，请稍后再试。")
        return

    message = "📈 **核心股票池AI分析报告**\n(按AI综合评分排序)\n--------------------------\n\n"
    for i, item in enumerate(report_data, 1):
        message += (
            f"🏅 #{i} **{item.get('name')} ({item.get('code')})**\n"
            f"  - AI评分: **{item.get('ai_score', 'N/A')} / 100**\n"
            f"  - AI点评: *{item.get('ai_comment', '无')}*\n\n"
        )
    
    # 防止消息过长，进行分段发送
    if len(message) > 4096:
        await update.message.reply_text("报告过长，将分段发送...")
        for i in range(0, len(message), 4096):
            await update.message.reply_text(message[i:i+4096], parse_mode='Markdown')
    else:
        await update.message.reply_text(message, parse_mode='Markdown')


def setup_handlers(application):
    """设置所有命令处理器"""
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("analyze", analyze_command))
    application.add_handler(CommandHandler("analyze_stocks", analyze_stocks_command))

