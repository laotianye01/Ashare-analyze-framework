from datetime import datetime
import threading
import time
from concurrent.futures import ProcessPoolExecutor, as_completed

import tasks
from core.logger import get_logger
from core.taskNode import TaskNode


class Pipeline:
    '''
    实例化的pipeline，用于管理器中的task有关的依赖，管理task的运行与依赖
    pipeline存在三种状态： hanging，executing，finishing
    在一个pipeline文件中，task需要按找依赖关系的顺序书写
    '''
    def __init__(self, pipeline_conf):        
        self.name = pipeline_conf.get("name")
        self.multi_launch = pipeline_conf.get("multi_launch")
        self.if_tmp = pipeline_conf.get("if_tmp")
        self.daily_launch_times = pipeline_conf.get("daily_launch_times")
        self.launch_interval_minutes = pipeline_conf.get("launch_interval_minutes")
        self.retry_interval = pipeline_conf.get("retry_interval")
        self.max_retry_times = pipeline_conf.get("max_retry_times")
        self.retry_times = 0
        self.status = 'hanging'
        self.last_exit = None
        self.task_map = {}
        logger_name = self.name + " logger"
        self.logger = get_logger(logger_name)
        
        self.lock = threading.Lock()
        
        # 构建 glob_param
        self.glob_param = self._build_task_glob_param(pipeline_conf)

        # 将所有的task加载到列表中
        for task_conf in pipeline_conf.get("tasks"):
            task = task_conf.get("task_class")
            task_name = task_conf.get("name")
            task_class = tasks.TASK_CLASS_REGISTRY.get(task)
            task = task_class(
                name_id=task_name,
                params=task_conf.get("params"),
                glob_params=self.glob_param,
                retry_interval=task_conf.get("retry_interval", 60),
            )
            self.task_map[task_name] = task

        # 加载完所有的task之后来初始化每一个task的dependency，此处的字典传递的为一个引用，所以需要使用深度复制
        for task_conf in pipeline_conf.get("tasks"):
            task = self.task_map[task_conf.get("name")]
            task.set_dependencies(task_conf.get("dependencies", {}), self.task_map)
            
    def _build_task_glob_param(self, pipeline_conf):
        glob_param = {}
        glob_param["save_path"] = pipeline_conf.get("save_path", "./cache/")
            
        return glob_param
            
    def get_all_tasks(self):
        return list(self.task_map.values())

    def get_ready_tasks(self):
        """遍历列表中task，调用task自身的函数判断当前task是否能被执行"""
        return [t for t in self.task_map.values() if t.status == "pending" and t.get_if_ready()]

    def get_if_has_pending_tasks(self):
        return any(t.status in ["pending", "failed"] for t in self.task_map.values())
    
    def get_if_can_start_now(self):
        now = datetime.now()
        if self.last_exit == None:
            return True
        if self.last_exit:
            elapsed = (now - self.last_exit).total_seconds() / 60
            if elapsed < self.launch_interval_minutes:
                return False
        if self.daily_launch_times and len(self.daily_launch_times) > 0:
            now_str = now.strftime('%H:%M')
            return now_str in self.daily_launch_times

        return True
    
    def get_state(self):
        """返回当前流水线的状态字典，并调用所有task的get_state。"""
        # 格式化 last_exit 时间，以确保可以被 JSON 序列化
        last_exit_iso = self.last_exit.isoformat() if self.last_exit else None
        
        return {
            "name": self.name,
            "status": self.status,
            "last_exit": last_exit_iso,
            "multi_launch": self.multi_launch,
            "tasks": {
                task_name: task_node.get_state()
                for task_name, task_node in self.task_map.items()
            }
        }
        
    def get_task(self, task_name):
        """
        根据任务名称从 task_map 中获取对应的 TaskNode 实例。
        """
        return self.task_map.get(task_name)
    
    # TODO:
    def set_params(self, param_dict: dict):
        """
        根据字典安全地更新 Pipeline 的成员变量，并进行类型验证。
        param_dict: 包含新参数名和值的字典。
        """
        with self.lock:
            for param, new_value in param_dict.items():
                if hasattr(self, param):
                    current_value = getattr(self, param)
                    # 检查当前值是否为 None
                    if current_value is None:
                        setattr(self, param, new_value)
                        self.logger.info(f"Updated pipeline '{self.name}' parameter '{param}' to '{new_value}'.")
                    # 如果当前值不为 None，则进行类型验证
                    elif type(new_value) == type(current_value):
                        setattr(self, param, new_value)
                        self.logger.info(f"Updated pipeline '{self.name}' parameter '{param}' to '{new_value}'.")
                    else:
                        self.logger.warning(f"Failed to update pipeline '{self.name}' parameter '{param}'. "
                                            f"Type mismatch: existing type '{type(current_value).__name__}', "
                                            f"new value type '{type(new_value).__name__}'.")
                else:
                    self.logger.warning(f"Pipeline '{self.name}' has no parameter '{param}'. Skipping.")
            # 更新完参数后将其状态设置为未完成
            self.status = 'hanging'

    # TODO: 支持外部传入json文件动态注册任务            
    def set_new_task(self, task_json):
        task_name = task_json.get("name")
        
        if task_name in self.task_map:
            raise ValueError(f"Task '{task_name}' already exists in pipeline '{self.name}'.")

        # 验证函数是否存在
        task_type = task_json.get("task_class")
        task_class = tasks.TASK_CLASS_REGISTRY.get(task_type)
        task = task_class(
            name_id=task_name,
            params=task_json.get("params"),
            retry_interval=task_json.get("retry_interval", 60),
        )

        # 添加到 task_map
        self.task_map[task_name] = task
        task.dep_map = task_json.get("dependencies", {})
        for dep_task_name in task.dep_map.keys():
            if dep_task_name in self.task_map:
                dep_task = self.task_map[dep_task_name]
                task.dependencies.add(dep_task)
            else:
                raise ValueError(f"Dependency task '{dep_task_name}' not found when adding task '{task_name}'.")

        self.logger.info(f"Dynamically added task '{task_name}' to pipeline '{self.name}'.")

    
class PipelineExecutor:
    '''
    用于并行执行pipeline中的task
    每一个pipeline为一个单独的进程，pipeline之间的task相互独立，不存在依赖关系
    每一个被实例化的pipeline执行器会启动两个线程，第一个线程用于task的执行，第二个线程用于错误任务的重启与检测

    该调度器是多线程的，其由_run执行线程，executor线程池与_monitor_and_dispatch监测线程所构成
    python中存在GIL（global interpreter locker），保证同一进程同一时间仅会编译一个字节码，也就导致了多线程变为了伪多线程
    但多进程并不受GIL的影响 -> 将 ThreadPoolExecutor 换成 ProcessPoolExecutor
    '''
    def __init__(self, pipeline: 'Pipeline', resource_conf, dynamic_loader, executor=None, debug=True):
        self.executor = executor if executor else ProcessPoolExecutor(max_workers=5)
        self.pipeline = pipeline
        self.resource_conf = resource_conf
        self.running = False
        logger_name = self.pipeline.name + " logger"
        self.logger = get_logger(logger_name)
        
        self.debug = debug
        self.dynamic_loader = dynamic_loader
        
    def run(self):
        """用于外部启动某个pipeline的执行器"""
        self.running = True
        threading.Thread(target=self._safe_run, daemon=True).start()
        
    def stop(self):
        """用于线程外部终止该进程的情况"""
        self.running = False
        self.executor.shutdown(wait=True)

    def _safe_run(self):
        """使用多进程执行task -> ProcessPoolExecutor适合在CPU 密集型或长耗时任务（如图像处理、模型推理）上使用，而非I/O密集型任务"""
        try:
            # 主线程启动
            while self.running:
                # get all ready tasks for main and sub pipelines
                ready_tasks = self.pipeline.get_ready_tasks()
                futures = {}

                for task in ready_tasks:
                    with task.lock:
                        task.status = "running"
                        if task.dep_map is not None:
                            task.set_task_params_base_on_dep()
                    task_class, name_id, params, glob_params = task.get_executor_args()
                    future = self.executor.submit(TaskNode.static_execute, task_class, self.resource_conf, name_id, params, glob_params)
                    futures[future] = task
                    
                # TODO: 目前不完全确定以下进程池中的task并行执行是否优化了运行效率
                for future in as_completed(futures):
                    task = futures[future]
                    try:
                        result = future.result()
                        if result["status"] == "success":
                            with task.lock:
                                task.status = "success"
                                if len(result["data"]) > 1:
                                    task.result = result["data"] 
                                if "next_tasks" in result:
                                    self.dynamic_loader.register_new_task(result.get("next_tasks"))
                                if "next_pipelines" in result:
                                    self.dynamic_loader.register_new_pipeline(result.get("next_pipelines"))              
                                self.logger.info(f"Task '{task.name_id}' completed.")
                                if self.debug:
                                    self.logger.info(f"Task '{task.name_id}' output: {result['data']}")
                        else:
                            with task.lock:
                                task.status = "failed"
                                task.last_failed_time = time.time()
                                task.retry_times += 1
                                self.logger.error(f"Task '{task.name_id}' failed with error: {result['error']}")
                    except Exception as e:
                        with task.lock:
                            task.status = "failed"
                            task.last_failed_time = time.time()
                            self.logger.error(f"Task '{task.name_id}' failed: {e}")
                            
                # 判断是否所的任务都实现了（包括动态注册文件中）
                if not self.pipeline.get_if_has_pending_tasks():
                    self.running = False

                time.sleep(1)
            self.logger.info(f"Finish {self.pipeline.name}")
        except Exception as e:
            self.pipeline.status = "fail"
            self.pipeline.retry_times += 1
            self.pipeline.last_exit = datetime.now()
            self.logger.exception(f"{self.pipeline.name} crashed with error", exc_info=e)
        finally:
            # 清理临时资源
            self.running = False
            self.pipeline.status = "success"
            self.pipeline.retry_times = 0
            self.pipeline.last_exit = datetime.now()
            
            if self.pipeline.if_tmp:
                for _, task in self.pipeline.task_map.items():
                    task.result = None
            self.logger.info(f"terminate {self.pipeline.name}")