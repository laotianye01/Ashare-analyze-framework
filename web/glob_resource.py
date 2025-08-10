# glob_resource.py
import threading
import multiprocessing
import os
from core.logger import get_logger
from web.util.pipeline_conf_manager import PipelineConfigManager

# --- 全局状态变量 ---
# 存储所有Scheduler实例及其通信通道的共享字典
scheduler_instances = {}
instance_lock = threading.Lock()

# --- 配置变量 ---
# 定义配置文件所在的文件夹路径
PIPELINE_CONFIG_FOLDER = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'entrance')
RESOURCE_CONFIG = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'config', 'resource', 'resource_conf.json')

# --- 进程管理 ---
# 启动一个全局管理器，用于创建共享队列
manager = multiprocessing.Manager()

# --- 应用程序实例和配置 ---
# 存储应用的配置，用于在蓝图和主应用中共享
app_config = {}

# --- app使用的logger ---
logger = get_logger("Webapp")

# --- app使用的配置管理器 ---
config_manager = PipelineConfigManager(PIPELINE_CONFIG_FOLDER)