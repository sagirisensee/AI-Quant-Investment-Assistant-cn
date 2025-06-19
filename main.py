import asyncio
import logging
import os
import sys
from pathlib import Path
from dotenv import load_dotenv
from bot_handler import setup_handlers  
from telegram.ext import Application, ApplicationBuilder


def load_config():
    """加载配置文件"""
    env_path = Path(__file__).parent / '.env'
    if not env_path.exists():
        logger.error(f" .env文件不存在: {env_path}")
        raise FileNotFoundError(f" .env文件不存在: {env_path}")
    
    load_dotenv(dotenv_path=env_path)
    token = os.getenv('TELEGRAM_TOKEN')
    if not token:
        logger.error(" 请在.env文件中配置TELEGRAM_TOKEN")
        raise ValueError(" 请在.env文件中配置TELEGRAM_TOKEN")
    return token

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)
logger = logging.getLogger(__name__)

# --- 这是主要修改的部分 ---
async def main():
    """使用async with启动和管理机器人"""
    try:
        TELEGRAM_TOKEN = load_config()
        
        # 使用上下文管理器构建和运行Application
        # async with 会自动处理 application.initialize() 和 application.shutdown()
        async with Application.builder().token(TELEGRAM_TOKEN).build() as application:
            setup_handlers(application)
            logger.info("🚀 ETF分析机器人启动... 按下 Ctrl+C 停止。")
            
            # run_polling()现在不再由我们直接调用，而是通过async with隐式管理
            # 我们只需要让这个协程保持运行即可
            await application.start()
            await application.updater.start_polling()
            
            # 保持主协程运行，直到被中断
            while True:
                await asyncio.sleep(3600) # 每小时唤醒一次，或者可以设置更长

    except (KeyboardInterrupt, SystemExit):
        logger.info("🛑 收到中断信号，机器人正在停止...")
    except Exception as e:
        logger.error(f"❌ 机器人运行出错: {e}", exc_info=True)
    finally:
        logger.info("🛑 机器人已停止。")


if __name__ == "__main__":
    print("=== ETF机器人启动 ===")
    print(f"Python路径: {sys.executable}")
    # 切换工作目录到脚本所在目录，避免路径问题
    os.chdir(Path(__file__).parent)
    print(f"工作目录: {os.getcwd()}")
    
    try:
        asyncio.run(main())
    except Exception as e:
        # 这个捕获是为了处理在main函数启动前就可能发生的错误，如配置加载失败
        print(f" 启动失败: {e}")
