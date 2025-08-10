from queue import Empty, Queue
import json
import os
import threading
import multiprocessing
import time
from concurrent.futures import ProcessPoolExecutor

from core.logger import get_logger
from core.pipeline import Pipeline, PipelineExecutor
     
        
class DynamicTaskLoader:
    '''
    用于动态加载任务与pipeline的类
    该类会监听一个配置文件的变化，并在文件变化时动态加载新的任务或pipeline配置
    '''
    def __init__(self, logger, new_pipeline_queue, new_task_queue, save_to_file=True):
        self.logger = logger
        self.last_mtime = 0
        self.new_pipeline_queue =new_pipeline_queue
        self.new_task_queue = new_task_queue
        self.save_to_file = save_to_file
        
        # 如果需要保存文件，则进行初始化
        if self.save_to_file:
            self._init_cache_file()
            self.lock = threading.Lock()
        else:
            self.lock = None

    def _init_cache_file(self):
        self.new_task_pipeline_json = "./cache/new_task_pipeline_json.json"
        
        with open(self.new_task_pipeline_json, "w", encoding="utf-8") as f:
            json.dump({"tasks": [], "pipelines": []}, f)
        self.logger.info(f"Dynamic registration cache file(for debug) initialized at: {self.new_task_pipeline_json}")
    
    # 用于注册新的任务,该函数会将新的任务配置添加到队列中，并在需要时将其保存到文件中   
    def register_new_task(self, conf: list):
        # 如果需要保存文件，则使用锁进行操作
        if self.save_to_file:
            with self.lock:
                with open(self.new_task_pipeline_json, "r+", encoding="utf-8") as f:
                    cache_data = json.load(f)
                    for new_task in conf:
                        cache_data["tasks"].append(new_task)
                    f.seek(0)
                    json.dump(cache_data, f, indent=4, ensure_ascii=False)
                    f.truncate()
                    
        # 如果不需要保存文件，直接将配置放入队列
        for new_task in conf:
            self.new_task_queue.put(new_task)

        self.logger.info(f"Added {len(conf)} new task(s) to the task queue.")

    def register_new_pipeline(self, conf: list):
        """
        用于注册新的流程,该函数会将新的流程配置添加到队列中，并在需要时将其保存到文件中
        """
        new_pipelines_to_put = []

        for pipeline_conf in conf:
            template_name = pipeline_conf.get("template")
            pipeline_params = pipeline_conf.get("params", {})
            if not template_name:
                self.logger.error("Missing 'template' in pipeline config.")
                continue

            template_path = "./config/tasks/" + template_name + ".json"
            try:
                with open(template_path, "r", encoding="utf-8") as f:
                    base_pipeline = json.load(f)
            except (FileNotFoundError, json.JSONDecodeError) as e:
                self.logger.error(f"Pipeline template '{template_name}' not found or invalid: {e}")
                continue

            new_pipeline_name = pipeline_conf.get("name")
            base_pipeline["name"] = new_pipeline_name
            
            # 修改子任务配置
            for task in base_pipeline.get("tasks", []):
                original_task_name = task.get("name")
                task["name"] = f"{new_pipeline_name}_{original_task_name}"
                task["params"] = task.get("params", {})
                
                for key, val in pipeline_params.items():
                    if key in task["params"]:
                        task["params"][key] = val

                task["retry_interval"] = task.get("retry_interval", 10)
                task["dependencies"] = {
                    f"{new_pipeline_name}_{k}": v for k, v in task["dependencies"].items()
                }
            
            new_pipelines_to_put.append(base_pipeline)

        # 批量放入队列
        for pipeline_to_put in new_pipelines_to_put:
            self.new_pipeline_queue.put(pipeline_to_put)
        self.logger.info(f"Added {len(new_pipelines_to_put)} new pipeline(s) to the pipeline queue.")

        # 如果需要保存文件，则写入
        if self.save_to_file:
            with self.lock:
                with open(self.new_task_pipeline_json, "r+", encoding="utf-8") as f:
                    cache_data = json.load(f)
                    for pipeline_to_put in new_pipelines_to_put:
                        cache_data["pipelines"].append(pipeline_to_put)
                    f.seek(0)
                    json.dump(cache_data, f, indent=4, ensure_ascii=False)
                    f.truncate()


class Scheduler:
    '''
    用于pipeline层面的调度，其将检测一个临时文件，用于pipeline的注册
    其同时将管理所有pipeline的生命周期
    我在scheduler与其启动的pipeline_exec.run()均为I/O密集任务，故将其作为主进程下子线程
    pipeline中对于task为计算密集型任务，故使用使用进程池

    pipeline 对象均被管理在主进程中，故其可以在主进程的不同子线程中共享状态
    '''
    def __init__(self, resource_config, pipeline_configs, state_queue=None, command_queue=None, updates_data=None):
        self.resource_conf = resource_config
        self.pipeline_confs = pipeline_configs
        self.running = False
        self.pipelines = []
        self.pipeline_exec = {}
        # TODO: 用于与webapp间通信
        self.state_queue = state_queue
        self.command_queue = command_queue
        # 新的task & pipeline队列，等待scheduler进行注册
        self.new_pipeline_queue = Queue()
        self.new_task_queue = Queue()
        self.logger = get_logger("Scheduler")
        # 处理全局的新任务注册请求
        self.dynamic_loader = DynamicTaskLoader(
            logger=self.logger, 
            new_pipeline_queue=self.new_pipeline_queue,
            new_task_queue=self.new_task_queue
        )
        # 使用全局进程池并设置总进程数上限
        self.global_executor = ProcessPoolExecutor(max_workers=16)  
        # 用于处理初始化时对配置的修改
        self.updates_data = updates_data
        
    def start(self):
        """用于外部启动调度器"""
        self.running = True
        # 执行项目的初始化（同时实例化Pipeline与PipelineExecutor）
        for pipeline_conf_path in self.pipeline_confs:
            with open(pipeline_conf_path, 'r', encoding='utf-8') as file:
                pipeline_conf = json.load(file)
                
            pipeline = Pipeline(pipeline_conf)
            self.pipelines.append(pipeline)
            self.pipeline_exec[pipeline] = PipelineExecutor(pipeline, self.resource_conf, self.dynamic_loader, executor=self.global_executor)
        
        # 如果初始化时对基础pipeline进行了修改
        if self.updates_data:
            self._apply_config_update(self.updates_data)
        
        threading.Thread(target=self._runner, daemon=True).start()
        threading.Thread(target=self._queue_monitor, daemon=True).start()
        threading.Thread(target=self._state_sender, daemon=True).start()
        # threading.Thread(target=self._command_listener, daemon=True).start()
        
    def stop(self):
        """用于外部终止调度器"""
        self.running = False
        
    def _queue_monitor(self):
        """
        监听新的task && pipeline注册请求队列，并实例化pipelines与pipeline_exec，以便runner进行调度
        """
        while self.running:
            # 先处理所有待添加的流水线
            while not self.new_pipeline_queue.empty():
                try:
                    pipeline_conf = self.new_pipeline_queue.get_nowait()
                    new_pipeline = Pipeline(pipeline_conf)
                    self.pipelines.append(new_pipeline)
                    self.pipeline_exec[new_pipeline] = PipelineExecutor(new_pipeline, self.resource_conf, self.dynamic_loader, executor=self.global_executor)
                    self.logger.info(f"Registered new pipeline '{new_pipeline.name}' from queue.")
                except Exception as e:
                    self.logger.error(f"Failed to register new pipeline from queue: {e}")

            # 然后处理所有待添加的任务
            while not self.new_task_queue.empty():
                try:
                    task_conf = self.new_task_queue.get_nowait()
                    task_name = task_conf.get("name")
                    task_pipeline_name = task_conf.get("pipeline_name", "")
                    
                    pipeline = next((p for p in self.pipelines if p.name == task_pipeline_name), None)
                    if pipeline is None:
                        self.logger.error(f"Target pipeline '{task_pipeline_name}' not found for task '{task_name}'.")
                        continue
                    with pipeline.lock:
                        if task_name not in pipeline.task_map:
                            pipeline.set_new_task(task_conf)
                            self.logger.info(f"New task '{task_name}' added to pipeline '{task_pipeline_name}'.")
                except Exception as e:
                    self.logger.error(f"Failed to add new task from queue: {e}")
            
            time.sleep(2) # 避免CPU空转
          
    # TODO: 用于监听webapp部分的操作  
    def _command_listener(self):
        """
        线程：监听并处理来自WebApp的配置更新命令。
        指令格式类似下面的形式
        {
            "updates": [
                {
                    "type": "pipeline",
                    "name": "your_pipeline_name",
                    "params": {
                        "launch_interval_minutes": 10
                    }
                },
                {
                    "type": "task",
                    "pipeline_name": "your_pipeline_name",
                    "task_name": "current_a_share_info",
                    "params": {
                        "save": 2
                    }
                }
            ]
        }
        """
        while self.running:
            try:
                # 使用带 timeout 的 get()，避免 CPU 空转
                command_data_str = self.command_queue.get(timeout=1)
                self.logger.info(f"Received command: {command_data_str}")
                command = json.loads(command_data_str)

                # 检查指令类型，目前只处理 "updates" 指令
                if "updates" in command:
                    self.logger.info("Processing config update command.")
                    self._apply_config_update(command)
                else:
                    self.logger.warning(f"Received unknown command format: {command}")

            except Empty:
                continue  # 队列为空，继续循环
            except json.JSONDecodeError as e:
                self.logger.error(f"Failed to decode command JSON: {e}")
            except Exception as e:
                self.logger.error(f"Error processing command: {e}")
                
    # TODO: 功能未实现
    def _apply_config_update(self, config_update):
        """
        根据解析后的 JSON 字典，修改对应的 pipeline 或 task 配置。更新格式需遵循以下定义
        """
        updates = config_update.get("updates", {})
        for item in updates:
            item_type = item.get("type")
            item_params = item.get("params", {})

            if item_type == "pipeline":
                pipeline_name = item.get("name")
                pipeline = next((p for p in self.pipelines if p.name == pipeline_name), None)
                if pipeline:
                    # 调用 Pipeline 实例的 set_params 方法
                    pipeline.set_params(item_params)
                else:
                    self.logger.warning(f"Pipeline '{pipeline_name}' not found for config update. Skipping.")

            elif item_type == "task":
                pipeline_name = item.get("pipeline_name")
                task_name = item.get("task_name")
                pipeline = next((p for p in self.pipelines if p.name == pipeline_name), None)
                if pipeline:
                    task = pipeline.get_task(task_name)
                    if task:
                        # 调用 TaskNode 实例的 set_params 方法
                        task.set_params(item_params)
                    else:
                        self.logger.warning(f"Task '{task_name}' not found in pipeline '{pipeline_name}'. Skipping.")
                else:
                    self.logger.warning(f"Pipeline '{pipeline_name}' not found for task config update. Skipping.")
            else:
                self.logger.warning(f"Unknown item type '{item_type}' in config update. Skipping.")
            
    def _remove_pipeline(self, pipeline):
        """用于移除已运行结束的pipeline"""
        if pipeline in self.pipeline_exec:
            del self.pipeline_exec[pipeline]
        if pipeline in self.pipelines:
            self.pipelines.remove(pipeline)
    
    def _runner(self):
        """该函数用于执行可执行的pipeline,应当判断当前pipeline是否需要重启或注销"""
        try: 
            while self.running:
                for pipeline in self.pipelines:
                    # Step 1: snapshot pipeline status（不加锁做“只读快照”）
                    try:
                        status = pipeline.status
                        can_start = pipeline.get_if_can_start_now()
                        all_tasks = pipeline.get_all_tasks()  # list 本身是浅拷贝
                    except Exception as e:
                        self.logger.error(f"Failed to read pipeline '{pipeline.name}': {e}")
                        continue

                    # Step 2: 写操作时加锁（只针对状态迁移或关键写操作）
                    if status == 'hanging' and can_start:
                        with pipeline.lock:
                            if pipeline.status == 'hanging':  # 二次判断，避免竞争冲突
                                pipeline.status = 'executing'
                                self.logger.info(f"execute workflow: {pipeline.name}")
                                self.pipeline_exec[pipeline].run()

                    elif status == 'success':
                        if pipeline.multi_launch:
                            with pipeline.lock:
                                pipeline.status = 'hanging'
                                self.logger.info(f"hang workflow: {pipeline.name}")
                        elif pipeline.if_tmp:
                            self._remove_pipeline(pipeline)
                            self.logger.info(f"remove workflow: {pipeline.name}")

                    elif status == 'fail':
                        if pipeline.retry_times >= pipeline.max_retry_times:
                            self._remove_pipeline(pipeline)
                            self.logger.info(f"remove workflow: {pipeline.name}")
                        elif can_start:
                            with pipeline.lock:
                                if pipeline.status == 'fail':
                                    pipeline.status = 'executing'
                                    self.logger.info(f"re-execute failed workflow: {pipeline.name}")
                                    self.pipeline_exec[pipeline].run()

                    # Step 3: 对 task 状态处理采用“逐个加锁”
                    for task in all_tasks:
                        if task.status == "failed" and time.time() - task.last_failed_time >= task.retry_interval:
                            with task.lock:
                                if task.retry_times >= task.max_retry:
                                    self.logger.info(f"{task.name_id} fails for maximum time {task.max_retry}, forced to terminate")
                                    task.status = "success"  # 或标记为 abandoned
                                else:
                                    self.logger.info(f"Retrying task: {task.name_id}")
                                    task.status = "pending"
                time.sleep(2)
        except Exception as e:
            self.logger.error(f"Error in monitor: {e}")
        finally:
            self.running = False
            # 删除缓存文件
            if self.new_task_pipeline_path and os.path.exists(self.new_task_pipeline_path):
                try:
                    os.remove(self.new_task_pipeline_path)
                    self.logger.info(f"Deleted task cache file: {self.new_task_pipeline_path}")
                except Exception as e:
                    self.logger.error(f"Failed to delete task cache file: {e}")
            self.logger.info(f"terminate monitor")
            
    def _state_sender(self):
        """定期将调度器状态快照发送到 WebApp 队列。该线程不加锁，因为它只读取实例状态"""
        while self.running:
            try:
                # 构建一个包含所有pipeline和task状态的字典快照（scheduler单位）
                scheduler_status = {
                    p.name: p.get_state()
                    for p in self.pipelines
                }
                
                # 非阻塞地将状态快照放入队列
                if self.state_queue:
                    self.state_queue.put_nowait(scheduler_status)

            except multiprocessing.queues.Full:
                self.logger.warning("State queue is full, skipping this status update.")
            except Exception as e:
                self.logger.error(f"Error in state sender thread: {e}")
            
            # 控制发送频率，例如每秒更新一次
            time.sleep(1)
            
# TODO: scheduler启动函数，供WebApp调用
def run_scheduler_process(scheduler_id, resource_conf, pipeline_json, state_queue, command_queue, updates_data):
    """
    Scheduler进程的入口函数。
    """
    try:
        scheduler = Scheduler(
            resource_config=resource_conf,
            pipeline_configs=[pipeline_json],
            state_queue=state_queue,
            command_queue=command_queue,
            updates_data=updates_data
        )
        scheduler.start()
        
        while True:
            time.sleep(1)
            
    except KeyboardInterrupt:
        pass
    finally:
        scheduler.stop()
        