# llm_analyzer.py (最终生产版 - 已修正Perplexity API的schema结构)

import os
import json
import logging
import asyncio
from openai import OpenAI

logger = logging.getLogger(__name__)

# --- 配置 ---
try:
    client = OpenAI(
        base_url=os.getenv("LLM_API_BASE"),
        api_key=os.getenv("LLM_API_KEY"),
    )
except Exception as e:
    logger.error(f"初始化OpenAI客户端失败，请检查.env配置: {e}")
    client = None

# --- 核心函数 ---
async def get_llm_score_and_analysis(etf_data, daily_trend_data):
    """调用大模型对单支ETF进行分析和打分"""
    if client is None:
        return None, "LLM服务未配置或初始化失败。"
        
    prompt_data = {
        "ETF名称": etf_data.get('name'), "代码": etf_data.get('code'),
        "日内涨跌幅": f"{etf_data.get('change', 0):.2f}%",
        "日线级别大趋势": daily_trend_data.get('status'),
        "盘中技术信号": etf_data.get('analysis_points')
    }

    system_prompt = (
        "你是一个专业的金融数据分析工具。请基于用户提供的JSON数据，客观地总结投资标的的状态，"
        "特别注意结合日线级别整体趋势、盘中技术信号以及**详细的技术指标分析**。"
        "详细技术指标分析包含：**均线的整体排列形态（如多头/空头排列/纠缠）**，股价与5日、10日、20日、60日均线关系，均线之间的金叉/死叉，60日均线趋势方向，以及MACD指标（金叉/死叉、零轴位置、红绿柱增减），"
        "以及60日成交量均线关系（如成交量较60日均量显著放大/萎缩）。"
        "综合这些信息，给出一个综合评分（0-100，50为中性）对于所有每一个获取到的指标的点评。请只输出JSON格式。"
        "请严格以JSON格式返回，包含'score'和'comment'两个键。"
    )

    try:
        # --- 关键修复：将json_schema内容正确地嵌套在"schema"键下 ---
        response = await asyncio.to_thread(
            client.chat.completions.create,
            model=os.getenv("LLM_MODEL_NAME", "sonar-pro"),
            messages=[
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": json.dumps(prompt_data, ensure_ascii=False, indent=2)}
            ],
            response_format={
                "type": "json_schema",
                "json_schema": {
                    "schema": { # <-- 关键的修复！
                        "type": "object",
                        "properties": {
                            "score": {"type": "number", "description": "0到100分的综合评分"},
                            "comment": {"type": "string", "description": "各指标得出的交易点评"}
                        },
                        "required": ["score", "comment"]
                    }
                }
            }
        )
        
        raw_content = response.choices[0].message.content
        if not raw_content:
            logger.warning(f"LLM为空内容返回: {etf_data.get('name')}")
            return 50, "模型未提供有效分析。"

        parsed_json = json.loads(raw_content)

        result_dict = None
        if isinstance(parsed_json, list) and parsed_json:
            result_dict = parsed_json[0]
        elif isinstance(parsed_json, dict):
            result_dict = parsed_json

        if result_dict and isinstance(result_dict, dict):
            score = result_dict.get('score')
            comment = result_dict.get('comment')
            if not isinstance(score, (int, float)):
                 score = 50
            return score, comment
        else:
            return None, "LLM返回格式错误"

    except Exception as e:
        logger.error(f"调用或解析LLM响应时出错: {e}", exc_info=True)
        return None, "LLM分析服务异常"