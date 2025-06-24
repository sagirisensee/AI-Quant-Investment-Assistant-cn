# AI驱动的ETF/股票分析机器人

## 项目概览

这是一个从零开始，逐步构建、调试并最终成功上线的高级AI驱动的ETF/股票分析机器人。它通过Telegram平台与用户交互，能够对您自定义的核心ETF和股票观察池进行深度分析，并提供由大语言模型（LLM）生成的专业评分和交易点评。

## 核心功能

*   **单一入口，全面分析**：用户只需发送简单的命令，即可触发全面的分析流程。
*   **混合分析模型**：机器人内部执行一套复杂的分析流程：
    *   **数据获取**：通过 `akshare` 库获取所有核心ETF和股票的实时行情数据及历史日线数据。
    *   **量化计算**：在本地计算客观的技术指标，包括日线趋势（如20日均线）和盘中异动信号（如相对成交量放大、日内大幅涨跌）。
    *   **AI推理**：将量化计算出的“结构化数据”打包，发送给大语言模型
    *   **生成报告**：接收AI返回的综合评分和精炼点评，并以清晰、美观的格式呈现给用户。
*   **双重分析模式**：同时支持对ETF和A股股票进行分析。

## 技术亮点与挑战应对

在开发过程中，我们克服了一系列典型的技术挑战，确保了程序的健壮性和可靠性：

*   **第三方库兼容性**：精准解决了 `pandas_ta` 库与 `akshare` 数据源之间因列名中英文不匹配（如 `close` vs `收盘`）导致的计算失败问题。
*   **API 访问策略**：有效应对了来自数据源和 LLM 服务商的双重 API 速率限制/反爬虫问题，通过引入“随机延迟”的“串行”请求，模拟人类行为，避免 IP 被封禁。
*   **API“方言”适配**：深入解决了不同 LLM 服务商 API 规范的细微差异，特别是 `response_format` 中关于 `json_schema` 的精确要求，确保了与兼容 OpenAI 格式的 API 无缝通信。如果所用api不支持`json_schema`格式，请在response_format处改为
            response_format={
                "type": "json_object"
            }

## 安装指南

### 前提条件

*   Python 3.10+ (推荐使用 Conda 管理环境)
*   Telegram 账号
*   一个兼容 OpenAI API 的大语言模型服务（例如 Perplexity AI, OpenAI GPT 等）的 API 密钥

### 安装步骤

1.  **克隆仓库**：
    ```
    git clone <您的仓库地址>
    cd <您的仓库名称>
    ```

2.  **创建并激活 Conda 环境** (推荐)：
    ```
    conda create -n etf_bot python=3.10
    conda activate etf_bot
    ```
    如果您不使用 Conda，可以直接创建 Python 虚拟环境：
    ```
    python3.10 -m venv venv
    source venv/bin/activate # macOS/Linux
    # 或 venv\Scripts\activate # Windows
    ```

3.  **安装依赖**：
    ```
    pip install -r requirements.txt
    ```

4.  **配置环境变量 (`.env` 文件)**：
    在项目根目录下创建 `.env` 文件，并填入以下配置信息。
    ```
    TELEGRAM_TOKEN="您的Telegram Bot Token"
    LLM_API_BASE="您的LLM API的基础URL
    LLM_API_KEY="您的LLM API Key"
    LLM_MODEL_NAME="您使用的LLM模型名称
    CACHE_EXPIRE_SECONDS="60" # 数据缓存有效期（秒），默认为60秒
    ```
    *   **Telegram Bot Token**: 从 BotFather 获取。
    *   **LLM_API_BASE, LLM_API_KEY, LLM_MODEL_NAME**: 根据您选择的LLM服务商获取。

5.  **运行机器人**：
    ```
    python main.py
    ```
    机器人启动后，您会在控制台看到日志输出，表示机器人已成功上线。

## 使用方法

在 Telegram 中找到您的机器人，并发送以下命令：

*   `/start` 或 `/help`：获取欢迎信息和可用命令列表。
*   `/analyze`：启动对您自定义的核心ETF池的AI分析，并生成报告。
*   `/analyze_stocks`：启动对您自定义的核心股票池的AI分析，并生成报告。

## 项目结构

*   `main.py`：机器人主入口文件，负责启动Telegram Bot。
*   `ak_utils.py`：数据获取模块，封装了 `akshare` 库的调用，负责获取ETF和股票的实时及历史数据，并包含数据缓存和重试逻辑。
*   `analysis.py`：核心分析引擎，整合了量化计算和LLM推理，负责生成ETF和股票的AI分析报告。
*   `llm_analyzer.py`：大语言模型分析器，负责与LLM API通信，发送结构化数据并解析LLM返回的评分和点评。
*   `bot_handler.py`：Telegram Bot 命令处理器，定义了处理用户命令（如 `/analyze`、`/analyze_stocks`）的异步函数。
*   `requirements.txt`：项目依赖库清单。
*   `.env`：环境变量配置文件（请勿提交到版本控制）。

## 自定义观察池

您可以通过修改 `ak_utils.py` 文件中的 `CORE_ETF_POOL` 和 `CORE_STOCK_POOL` 列表，来增删您希望机器人分析的ETF和股票。

示例：
``` text
ak_utils.py
CORE_ETF_POOL = [
{'code': '510050', 'name': '上证50ETF'},
# 添加或删除ETF
]

CORE_STOCK_POOL = [
{'code': '600519', 'name': '贵州茅台'},
# 添加或删除股票
]
```
