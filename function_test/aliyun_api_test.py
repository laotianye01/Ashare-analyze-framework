import csv
import json
import os
from openai import OpenAI
from tqdm import tqdm
# 模型api提供平台https://bailian.console.aliyun.com/

# 读取 CSV 文件并加载为 news_list
def load_news_from_csv(file_path):
    news_list = []
    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        for row in reader:
            news_list.append(row['内容'])  # 假设 CSV 文件中新闻内容在 "内容" 列
    return news_list

# 将模型输出保存到 CSV 文件
def save_to_csv(data, file_path):
    with open(file_path, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.writer(file)
        writer.writerow(['消息主体', '消息内容概括'])
        for item in data:
            if ':' in item:
                subject, summary = item.split(':', 1)  # 仅分割一次
                writer.writerow([subject.strip(), summary.strip()])

# 将模型输出保存到 JSON 文件
def save_to_json(data, file_path):
    with open(file_path, mode='w', encoding='utf-8') as file:
        json.dump(data, file, ensure_ascii=False, indent=4)

client = OpenAI(
    # 若没有配置环境变量，请用百炼API Key将下行替换为：api_key="sk-xxx",
    api_key="", 
    base_url="",
)    
    
# 主函数
def obtain_ali_ai_res():
    # 读取新闻数据
    news_list = load_news_from_csv('./data/daily_news.csv')
    
    # 分批处理新闻
    BATCH_SIZE = 10
    results = []
    for i in tqdm(range(0, len(news_list), BATCH_SIZE), total=len(news_list) / BATCH_SIZE, desc="Processing batches"):
        batch_news = news_list[i:i + BATCH_SIZE]
        batch_content = "\n".join(batch_news)
        
        completion = client.chat.completions.create(
            model="qwen-plus",
            messages=[
                {
                    'role': 'system',
                    'content': (
                        '你将作为我的金融新闻分析员。我将提供若干条财经新闻，请你基于内容，提取每条新闻的关键信息，并将其归入以下消息主体之一：'
                        '股票对应代码（如300059）、央行、财政、国际新闻、国内新闻。'
                        '请重点关注与股票市场、重要经济数据、政策调整、国际经济形势相关的内容。'
                        '请不要编造不存在的内容。输出格式必须严格按照以下格式：\n'
                        '<消息主体>: 消息内容概括。\n，以下为对应的样例，同时不要在输出时添加任何与概括无关的内容，包括标题。\n'
                        '月之暗面,公司即将推出首个内容社区产品，探索AI战略新方向。\n'
                        'SK海力士,因HBM需求增加，上调年度资本开支至29万亿韩元。\n'
                        '每条新闻只对应一个消息主体。请确保语言简洁，概括准确，每条概括不超过50字。'
                    )
                },
                {
                    'role': 'user',
                    'content': batch_content
                }
            ]
        )
        
        # 解析模型输出
        output = completion.model_dump_json()
        output_data = json.loads(output)  # 解析 JSON 数据
        if 'choices' in output_data and output_data['choices']:
            for choice in output_data['choices']:
                if 'message' in choice and 'content' in choice['message']:
                    news_items = choice['message']['content'].strip().split('\n')
                    # 将每一条存入 results 列表
                    for item in news_items:
                        results.append(item)

    # 保存结果到 CSV 文件
    save_to_csv(results, './data/ai_news.csv')

    # # 保存结果到 JSON 文件
    # save_to_json(results, 'output.json')

if __name__ == "__main__":
    obtain_ali_ai_res()
