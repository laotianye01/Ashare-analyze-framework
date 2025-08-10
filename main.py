import sys
from core.resource_manager import ResourceManager
from core.scheduler import Scheduler
from core.logger import get_logger
import signal
import time
import os

# 把项目根目录加入 sys.path
project_root = os.path.dirname(os.path.abspath(__file__))
sys.path.append(project_root)

# 获取主程序的日志记录器
logger = get_logger("main")

# TODO: 实现系统所需的GUI
# TODO: 数据库插入部分新增逻辑 -- 在插入前判断数据是否应当被插入(在taskNode中统一进行修改)
if __name__ == "__main__":
    logger.info("Starting the application")
    resource_conf = './config/resource/resource_conf.json'
    pipeline_json = './config/tasks/test_workflow.json'

    # 初始化并启动调度器
    scheduler = Scheduler(resource_config=resource_conf, pipeline_configs=[pipeline_json])
    scheduler.start()
    logger.info("Scheduler started. Press Ctrl+C to exit.")

    # 等待中断信号
    def handle_interrupt(signum, frame):
        logger.info("Shutting down scheduler...")
        scheduler.stop()
        exit(0)

    signal.signal(signal.SIGINT, handle_interrupt)
    signal.signal(signal.SIGTERM, handle_interrupt)

    # 主线程阻塞等待
    while True:
        time.sleep(1)
