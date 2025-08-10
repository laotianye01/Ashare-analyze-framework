# webapp_main.py (修正后)
import multiprocessing
import logging
# 从 web.webapp 导入 create_app
from web.webapp import create_app

logger = logging.getLogger("Main")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
sh = logging.StreamHandler()
sh.setFormatter(formatter)
logger.addHandler(sh)

if __name__ == "__main__":
    logger.info("以WebApp作为核心管理器启动应用...")
    
    # 直接在主进程中创建并运行Flask应用
    app = create_app()
    logger.info("WebApp正在运行。按 Ctrl+C 退出。")

    try:
        # app.run() 会阻塞，直到收到终止信号，这确保了主进程保持活动状态，直到它主动终止
        # app.run(host='::', port=5000, debug=False, use_reloader=False)
        app.run(host='0.0.0.0', port=5000, debug=False, use_reloader=False)
    except KeyboardInterrupt:
        logger.info("主进程接收到中断信号。cleanup_schedulers 函数应当被调用。")
        # 由于 signal.signal 已经注册，此处不需要额外清理代码
        pass

    logger.info("主进程退出。")