from telegram import Update
from telegram.ext import CommandHandler, ContextTypes
import logging
from analysis import generate_ai_driven_report#,get_detailed_analysis_report_for_debug
from ak_utils import CORE_ETF_POOL, CORE_STOCK_POOL, get_all_etf_spot_realtime, get_etf_daily_history, get_all_stock_spot_realtime, get_stock_daily_history

logger = logging.getLogger(__name__)

# --- 消息发送辅助函数 (已简化) ---
async def send_long_message(update: Update, text: str):
    """发送长消息，自动分割，纯文本模式。"""
    if len(text) <= 4096:
        await update.message.reply_text(text)
        return

    parts = []
    while len(text) > 0:
        if len(text) > 4096:
            split_pos = text[:4096].rfind('\n')
            if split_pos == -1:
                split_pos = 4096
            
            parts.append(text[:split_pos])
            text = text[split_pos:].lstrip()
        else:
            parts.append(text)
            break
            
    for part in parts:
        await update.message.reply_text(part)

# --- 命令处理器 ---
async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """欢迎信息"""
    welcome_text = (
        "🚀 AI驱动的量化投资助手\n"
        "--------------------------\n"
        "我将为您核心观察池中的所有投资标的提供由大语言模型生成的综合评分和交易点评。\n\n"
        "📌 可用命令:\n"
        "/analyze - 开始ETF分析\n"
        "/analyze_stocks - 开始股票分析\n"
        "/debug_analyze - ETF调试分析 (无AI)\n"
        "/debug_stocks - 股票调试分析 (无AI)\n"
        "/help - 显示此帮助信息"
    )
    await update.message.reply_text(welcome_text)

async def analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行全面的ETF AI分析"""
    logger.info("收到 /analyze 命令，启动ETF AI分析...")
    await update.message.reply_text("好的，正在为您启动ETF分析引擎...")
    
    report_data = await generate_ai_driven_report(
        get_realtime_data_func=get_all_etf_spot_realtime,
        get_daily_history_func=get_etf_daily_history,
        core_pool=CORE_ETF_POOL
    )
    
    if not report_data:
        await update.message.reply_text("未能生成ETF AI分析报告，请稍后再试。")
        return

    message_header = "🤖 核心ETF池AI分析报告\n(按AI综合评分排序)\n--------------------------\n\n"
    message_body = ""
    for i, item in enumerate(report_data, 1):
        ai_comment = item.get('ai_comment')
        if ai_comment is None:
            ai_comment = "无"

        message_body += (
            f"🏅 #{i} {item.get('name')} ({item.get('code')})\n"
            f"  - AI评分: {item.get('ai_score', 'N/A')} / 100\n"
            f"  - AI点评: {ai_comment}\n\n"
        )
    
    final_message = message_header + message_body
    await send_long_message(update, final_message)

async def analyze_stocks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """执行全面的股票AI分析"""
    logger.info("收到 /analyze_stocks 命令，启动股票AI分析...")
    await update.message.reply_text("好的，正在为您启动股票分析引擎...")
    
    report_data = await generate_ai_driven_report(
        get_realtime_data_func=get_all_stock_spot_realtime,
        get_daily_history_func=get_stock_daily_history,
        core_pool=CORE_STOCK_POOL
    )
    
    if not report_data:
        await update.message.reply_text("未能生成股票AI分析报告，请稍后再试。")
        return

    message_header = "📈 核心股票池AI分析报告\n(按AI综合评分排序)\n--------------------------\n\n"
    message_body = ""
    for i, item in enumerate(report_data, 1):
        ai_comment = item.get('ai_comment')
        if ai_comment is None:
            ai_comment = "无"
            
        message_body += (
            f"🏅 #{i} {item.get('name')} ({item.get('code')})\n"
            f"  - AI评分: {item.get('ai_score', 'N/A')} / 100\n"
            f"  - AI点评: {ai_comment}\n\n"
        )
    
    final_message = message_header + message_body
    await send_long_message(update, final_message)
    '''
async def debug_analyze_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """ETF调试分析（仅量化，不调用AI）"""
    logger.info("收到 /debug_analyze 命令，启动ETF调试分析...")
    await update.message.reply_text("正在生成ETF调试分析报告（仅量化，不调用AI）...")

    report_data = await get_detailed_analysis_report_for_debug(
        get_realtime_data_func=get_all_etf_spot_realtime,
        get_daily_history_func=get_etf_daily_history,
        core_pool=CORE_ETF_POOL
    )

    if not report_data:
        await update.message.reply_text("未能生成ETF调试报告，请稍后再试。")
        return

    message_header = "🛠 ETF调试分析报告（仅量化）\n--------------------------\n\n"
    message_body = ""
    for i, item in enumerate(report_data, 1):
        tech_summary = "\n    ".join(item.get('technical_indicators_summary', []))
        intraday = ", ".join(item.get('intraday_signals', []))
        message_body += (
            f"#{i} {item.get('name')} ({item.get('code')})\n"
            f"  - 最新价: {item.get('price', 'N/A')}\n"
            f"  - 涨跌幅: {item.get('change', 'N/A')}\n"
            f"  - 盘中信号: {intraday}\n"
            f"  - 日线趋势: {item.get('daily_trend_status', '未知')}\n"
            f"  - 技术指标摘要:\n    {tech_summary}\n\n"
        )
    final_message = message_header + message_body
    await send_long_message(update, final_message)

async def debug_stocks_command(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """股票调试分析（仅量化，不调用AI）"""
    logger.info("收到 /debug_stocks 命令，启动股票调试分析...")
    await update.message.reply_text("正在生成股票调试分析报告（仅量化，不调用AI）...")

    report_data = await get_detailed_analysis_report_for_debug(
        get_realtime_data_func=get_all_stock_spot_realtime,
        get_daily_history_func=get_stock_daily_history,
        core_pool=CORE_STOCK_POOL
    )

    if not report_data:
        await update.message.reply_text("未能生成股票调试报告，请稍后再试。")
        return

    message_header = "🛠 股票调试分析报告（仅量化）\n--------------------------\n\n"
    message_body = ""
    for i, item in enumerate(report_data, 1):
        tech_summary = "\n    ".join(item.get('technical_indicators_summary', []))
        intraday = ", ".join(item.get('intraday_signals', []))
        message_body += (
            f"#{i} {item.get('name')} ({item.get('code')})\n"
            f"  - 最新价: {item.get('price', 'N/A')}\n"
            f"  - 涨跌幅: {item.get('change', 'N/A')}\n"
            f"  - 盘中信号: {intraday}\n"
            f"  - 日线趋势: {item.get('daily_trend_status', '未知')}\n"
            f"  - 技术指标摘要:\n    {tech_summary}\n\n"
        )
    final_message = message_header + message_body
    await send_long_message(update, final_message)
    '''
def setup_handlers(application):
    """设置所有命令处理器"""
    application.add_handler(CommandHandler("start", start))
    application.add_handler(CommandHandler("help", start))
    application.add_handler(CommandHandler("analyze", analyze_command))
    application.add_handler(CommandHandler("analyze_stocks", analyze_stocks_command))
    '''
    application.add_handler(CommandHandler("debug_analyze", debug_analyze_command))
    application.add_handler(CommandHandler("debug_stocks", debug_stocks_command))
    '''