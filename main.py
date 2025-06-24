import asyncio
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from bot_handler import setup_handlers  
from telegram.ext import Application, ApplicationBuilder

# --- 这是主要修改的部分 ---
def load_config():
    """加载配置文件"""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        # logger 在这里还未完全配置，所以使用print
        print(f"❌ .env文件不存在: {env_path}")
        raise FileNotFoundError(f" .env文件不存在: {env_path}")
    
    # 1. 强制覆盖已有的环境变量，确保每次都从.env文件加载最新配置
    load_dotenv(dotenv_path=env_path, override=True)
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        print("❌ 请在.env文件中配置TELEGRAM_TOKEN")
        raise ValueError("❌ 请在.env文件中配置TELEGRAM_TOKEN")
    return token

# 2. 配置日志，同时输出到控制台和文件
log_formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger()
logger.setLevel(logging.INFO)

# 控制台处理器
console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(log_formatter)
logger.addHandler(console_handler)

# 文件处理器 (写入到 bot.log 文件)
file_handler = logging.FileHandler("bot.log", mode='a', encoding='utf-8')
file_handler.setFormatter(log_formatter)
logger.addHandler(file_handler)


async def main():
    """使用async with启动和管理机器人"""
    try:
        TELEGRAM_TOKEN = load_config()
        
        async with Application.builder().token(TELEGRAM_TOKEN).build() as application:
            setup_handlers(application)
            logger.info("🚀 AI量化投资助手启动... 按下 Ctrl+C 停止。")
            
            await application.start()
            await application.updater.start_polling()
            
            while True:
                await asyncio.sleep(3600)

    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 收到中断信号，机器人正在停止...")
    except Exception as e:
        logger.error(f"❌ 机器人运行出错: {e}", exc_info=True)
    finally:
        logger.info("🛑 机器人已停止。")


if __name__ == "__main__":
    print("=== AI量化投资助手启动脚本 ===")
    
    # 切换工作目录到脚本所在目录，避免路径问题
    os.chdir(Path(__file__).parent)
    print(f"工作目录: {os.getcwd()}")
    
    try:
        asyncio.run(main())
    except Exception as e:
        print(f"❌ 启动失败: {e}")
