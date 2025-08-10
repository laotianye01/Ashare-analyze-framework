import json
import os
from core.logger import get_logger

logger = get_logger("ResourceManager")


def load_task_from_folder(config_dir):
    """从配置文件夹加载任务并注册到调度器"""
    task_list = []
    # 遍历配置文件夹中的所有 JSON 文件
    for filename in os.listdir(config_dir):
        if filename.endswith('.json'):
            config_path = os.path.join(config_dir, filename)
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    # 读取内容并去除首尾空白 + 检查是否为空(该操作会使f指针移动到文件的末尾，导致直接使用load函数报错)
                    content = f.read().strip()  
                    if not content:  
                        logger.warning(f"警告: 文件 {filename} 为空，已跳过")
                        continue
                    
                    config = json.loads(content)
                    for task_config in config.get("tasks", []):
                        task_list.append(task_config)
            except json.JSONDecodeError as e:
                logger.error(f"JSON 解析失败: {filename} - 错误: {e}")
            
    return task_list

def load_tasks_from_config(config_dir, scheduler):
    """
    从配置文件夹加载任务,并注册到调度器
    """
    import tasks
    task_list = load_task_from_folder(config_dir)
    for task in task_list:
        # 动态加载任务模块，注册任务到调度器
        try:
            task_func = getattr(tasks, task.get("func_name"), None)
            if task_func:
                scheduler.add_task(task_func, task.get("schedule_time"), task.get("params", task.get("type")))
            else:
                logger.error(f"Function '{task.get("func_name")}' not found in module tasks.")
        except ModuleNotFoundError:
            logger.error(f"tasks module not found.")
        except Exception as e:
            logger.error(f"Error loading task '{task.get("func_name")}': {e}")