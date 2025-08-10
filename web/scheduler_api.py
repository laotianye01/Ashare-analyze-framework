import os
import uuid
import multiprocessing
from flask import Blueprint, jsonify, request
from multiprocessing import Process

# 假设这些全局变量和函数可以在其他文件中导入
from web.glob_resource import scheduler_instances, instance_lock, manager, RESOURCE_CONFIG, logger
from core.scheduler import run_scheduler_process

# 创建一个名为 'scheduler_api' 的蓝图，蓝图可被统一的注册到webapp当中
# 所有路由的URL前缀将为 /schedulers，用于处理用户端所有前缀为scheduler的请求
scheduler_bp = Blueprint('scheduler_api', __name__, url_prefix='/schedulers')

# 以下函数用于将scheduler的最新数据发送给前端
# 当http GET请求访问/schedulers路径时，以下函数会被调用
@scheduler_bp.route('/', methods=['GET'])
def list_schedulers():
    """
    列出所有正在运行的Scheduler实例及其状态（存储于scheduler实例字典中的latest_state）
    """
    with instance_lock:
        response = {
            sch_id: instance['latest_state']
            for sch_id, instance in scheduler_instances.items()
        }
    return jsonify(response)
    
# 外部 HTTP POST 请求访问 /schedulers/create 时，该函数会被调用
# TODO：判断request中是否包含所需信息
@scheduler_bp.route('/create', methods=['POST'])
def create_scheduler():
    """
    动态创建并启动一个新的Scheduler实例。请求中应当包括pipeline_path选项
    """
    data = request.get_json()
    resource_conf = RESOURCE_CONFIG
    # TODO:此处应当通过json选项动态构建配置路径 
    pipeline_json = data.get("pipeline_path")
    if not pipeline_json:
        return jsonify({"error": "Missing pipeline_path in request body"}), 400
    
    # 获取 updates 列表，如果不存在则默认为空列表 []
    updates_data = data.get("updates", {})
    logger.info("updates_data: : %s", updates_data)
    
    # 创建scheduler进程
    scheduler_id = str(uuid.uuid4())
    # 为新的Scheduler创建独立的通信队列
    state_queue = manager.Queue()
    command_queue = manager.Queue()
    
    proc = Process(
        target=run_scheduler_process,
        args=(scheduler_id, resource_conf, pipeline_json, state_queue, command_queue, updates_data),
        name=f"SchedulerProcess-{scheduler_id}"
    )
    
    with instance_lock:
        scheduler_instances[scheduler_id] = {
            'process': proc,
            'state_queue': state_queue,
            'command_queue': command_queue,
            'latest_state': {}
        }
        
    proc.start()
    
    return jsonify({"message": f"Scheduler '{scheduler_id}' created and started."}), 201

# TODO: 当 HTTP PUT 请求访问 /schedulers/<scheduler_id>/config 时，该请求会被调用(用于修改特定实例中的配置信息)
@scheduler_bp.route('/<string:scheduler_id>/config', methods=['PUT'])
def update_scheduler_config(scheduler_id):
    """
    修改指定Scheduler实例的配置。
    """
    config_update_command = request.get_json()
    
    with instance_lock:
        if scheduler_id not in scheduler_instances:
            return jsonify({"error": f"Scheduler '{scheduler_id}' not found."}), 404
        
        instance = scheduler_instances[scheduler_id]
        
        try:
            instance['command_queue'].put_nowait(config_update_command)
            return jsonify({"message": f"Config update sent to scheduler '{scheduler_id}'."}), 200
        except multiprocessing.queues.Full:
            return jsonify({"error": "Config queue is full. Try again later."}), 503

# TODO：实现用户端对应的请求
@scheduler_bp.route('/<string:scheduler_id>/stop', methods=['POST'])
def stop_scheduler(scheduler_id):
    """
    停止指定的Scheduler实例。
    """
    with instance_lock:
        if scheduler_id not in scheduler_instances:
            logger.error(f"Scheduler '{scheduler_id}' not found.")
            return jsonify({"error": f"Scheduler '{scheduler_id}' not found."}), 404
        
        proc = scheduler_instances[scheduler_id]['process']
        proc.terminate()
        proc.join()
        del scheduler_instances[scheduler_id]
        logger.info(f"Scheduler '{scheduler_id}' removed.")
        
    return jsonify({"message": f"Scheduler '{scheduler_id}' stopped and removed."}), 200