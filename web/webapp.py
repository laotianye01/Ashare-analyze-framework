import os
import signal
import sys
import threading
import time
from flask import Flask, render_template
import multiprocessing

from core.logger import get_logger
# 从 routes 文件夹导入蓝图
from web.glob_resource import scheduler_instances, instance_lock, logger
from web.scheduler_api import scheduler_bp
from web.config_api import config_bp

# 定义主 WebApp 线程，用于获取状态
def update_all_states():
    """
    线程：持续从所有Scheduler实例的队列中获取最新的状态并将其赋值给webapp管理的scheduler实例字典
    """
    while True:
        with instance_lock:
            for sch_id, instance in scheduler_instances.items():
                try:
                    latest_state = instance['state_queue'].get_nowait()
                    instance['latest_state'] = latest_state
                except multiprocessing.queues.Empty:
                    continue
                except Exception as e:
                    logger.error(f"Error updating state for scheduler {sch_id}: {e}")
        time.sleep(1)

def cleanup_schedulers(signum, frame):
    """SIGINT 信号处理器，用于终止所有子进程。"""
    logger.info("SIGINT received. Terminating all scheduler processes...")

    with instance_lock:
        # 使用 list() 创建字典键的副本，以便安全地在循环中修改字典
        for sch_id in list(scheduler_instances.keys()):
            instance = scheduler_instances[sch_id]
            proc = instance['process']

            try:
                if proc.is_alive():
                    logger.info(f"Terminating scheduler {sch_id}...")

                    proc.terminate()
                    proc.join(timeout=5)  # 等待5秒钟

                    # 如果5秒后进程仍然存活，则强制杀死它
                    if proc.is_alive():
                        logger.warning(f"Scheduler {sch_id} did not terminate gracefully, killing...")
                        proc.kill()

            except Exception as e:
                logger.error(f"Error terminating process {sch_id}: {e}")
            finally:
                # 无论如何，都从字典中移除该实例
                del scheduler_instances[sch_id]

    logger.info("All schedulers terminated. Exiting WebApp.")
    sys.exit(0)

def create_app():
    app = Flask(__name__)
    
    # 注册 SIGINT 信号处理器
    signal.signal(signal.SIGINT, cleanup_schedulers)
    
    # 创建一个后台线程来定期从所有Scheduler获取状态
    state_updater_thread = threading.Thread(target=update_all_states, daemon=True)
    state_updater_thread.start()

    # 在主应用中注册蓝图
    # url_prefix='/schedulers' 将蓝图中所有路由的URL前面都加上 /schedulers
    app.register_blueprint(scheduler_bp)
    app.register_blueprint(config_bp)
    
    @app.route('/')
    def index():
        return render_template('index.html')
    
    return app