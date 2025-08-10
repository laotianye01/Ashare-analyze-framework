import base64
import json
import os
import threading

import pandas as pd
from core.logger import get_logger
from core.resource_manager import ResourceManager
from utils.database_utils import insert_dataframe_to_table


class TaskNode:
    '''
    对接task json的接口，读取并加载对应的task
    task的状态分为pending，ready，running，finish，error
    tasknode对外暴露了get / set等方法外加execute方法

    由于该项目较小，所以未使用开闭原则与单一职能原则设计tasknode
    如希望使用对应原则，则可创建command抽象类，并将如执行，存储，注册或是通信等command类子类
    在execute中，通过解析对应task的执行参数判断对于各个command的执行逻辑
    '''
    def __init__(self, name_id=None, params=None, glob_params=None, retry_interval=60, max_retry=2):
        # self.func_name = func_name              # 该变量保存了函数名
        self.name_id = name_id                  # 给定pipeline中task唯一的标识符
        self.params = params or {}
        
        self.glob_params = glob_params or {}
        
        self.dependencies = set()
        self.dep_map = None                     # 用于表示函数输入与其余函数输出之间的对应关系
        self.status = "pending"
        self.retry_times = 0
        self.last_failed_time = None
        self.retry_interval = retry_interval
        self.max_retry = max_retry
        self.result = None
        self.lock = threading.Lock()
        
        self.logger = get_logger("task logger")
     
    def set_dependencies(self, dep_map, task_map):
        """用于从配置文件设置当前task的依赖关系"""
        self.dep_map = dep_map
        for dep_task in list(self.dep_map.keys()):
            if dep_task in task_map:
                dep_task = task_map[dep_task]
                self.dependencies.add(dep_task)
            else:
                raise ValueError(f"Task {dep_task} not in pipeline '{self.name}'")
            
    def set_task_params_base_on_dep(self):
        """当当前任务状态为ready时，依照任务节点的依赖更新任务参数   """
        # 遍历已存储的依赖任务实例
        for dep_task in self.dependencies:
            dep_task_name = dep_task.name_id
            target_param = self.dep_map.get(dep_task_name)

            # 如果 target_param 为空字符串，则表示仅为先后顺序依赖
            if not target_param:
                continue

            dep_result = dep_task.result
            # TODO: 以下有关task传入参数的更新方法覆盖并不完全，需要进行一定的更新
            if dep_result is None:
                # 依赖任务已完成但结果为空，可能存在逻辑问题，或者依赖任务本身无返回值
                raise ValueError(f"Dependency result for '{dep_task_name}' is None.")

            # 更新当前任务的参数
            if isinstance(self.params.get(target_param), str) and isinstance(dep_result, str):
                self.params[target_param] += dep_result
            elif isinstance(self.params.get(target_param), str) and isinstance(dep_result, pd.DataFrame):
                # 如果 dep_result 是 DataFrame，将其转换为字符串
                dep_result_str = dep_result.to_string()
                self.params[target_param] += dep_result_str
            elif isinstance(self.params.get(target_param), dict):
                self.params[target_param][dep_task_name] = dep_result
            else:
                self.params[target_param] = dep_result
                
    def set_params(self, param_dict: dict):
        """
        根据字典更新 TaskNode 的成员变量和内部 params 字典，并进行类型验证。
        param_dict: 包含新参数名和值的字典。
        """
        with self.lock:
            for param, new_value in param_dict.items():
                # 优先检查是否为内部 params 字典中的键
                if param in self.params:
                    current_value = self.params[param]
                    # 检查当前值是否为 None
                    if current_value is None:
                        self.params[param] = new_value
                        self.logger.info(f"Updated task '{self.name_id}' internal param '{param}' to '{new_value}'.")
                    # 如果当前值不为 None，则进行类型验证
                    elif type(new_value) == type(current_value):
                        self.params[param] = new_value
                        self.logger.info(f"Updated task '{self.name_id}' internal param '{param}' to '{new_value}'.")
                    else:
                        self.logger.warning(f"Failed to update task '{self.name_id}' internal param '{param}'. "
                                            f"Type mismatch: existing type '{type(current_value).__name__}', "
                                            f"new value type '{type(new_value).__name__}'.")
                # 其次检查是否为 TaskNode 实例的直接属性
                elif hasattr(self, param):
                    current_value = getattr(self, param)
                    if current_value is None:
                        setattr(self, param, new_value)
                        self.logger.info(f"Updated task '{self.name_id}' attribute '{param}' to '{new_value}'.")
                    elif type(new_value) == type(current_value):
                        setattr(self, param, new_value)
                        self.logger.info(f"Updated task '{self.name_id}' attribute '{param}' to '{new_value}'.")
                    else:
                        self.logger.warning(f"Failed to update task '{self.name_id}' attribute '{param}'. "
                                            f"Type mismatch: existing type '{type(current_value).__name__}', "
                                            f"new value type '{type(new_value).__name__}'.")
                else:
                    self.logger.warning(f"Task '{self.name_id}' has no parameter or attribute '{param}'. Skipping.")
            # 跟新参数后将task节点的状态更新     
            self.status = "pending"
            
    def get_if_ready(self) -> bool:
        """用于检测其所有依赖是否已完成运行"""
        return all(dep.status == "success" for dep in self.dependencies)
    
    def get_executor_args(self):
        """用于获得当前task实例的所有运行与实例化所需参数，用于可序列化的向多进程调度器传入参数"""
        return self.__class__, self.name_id, self.params, self.glob_params
    
    # ---------------------------- 用于webapp端获取节点信息所使用 ----------------------------
    def _process_result(self, result):
        """
        根据result的类型，将其转换为前端友好的格式。
        """
        if isinstance(result, str):
            # 如果是字符串，直接返回
            return {"type": "text", "content": result}
        
        elif isinstance(result, pd.DataFrame):
            # 如果是DataFrame，转换为JSON字符串
            return {"type": "dataframe", "content": result.to_json(orient='split')}
        
        # 假设图片类型为bytes
        elif isinstance(result, bytes):
            # 转换为base64编码的字符串
            encoded_image = base64.b64encode(result).decode('utf-8')
            return {"type": "image", "content": encoded_image}
        
        # 如果是可序列化的对象，转换为JSON字符串
        try:
            json_content = json.dumps(result)
            return {"type": "json", "content": json_content}
        except (TypeError, OverflowError):
            # 如果无法序列化，返回一个表示错误的字符串
            return {"type": "error", "content": "Result could not be serialized."}
        
    def get_state(self):
        """返回当前任务的状态字典，并进行必要的格式转换。"""
        # 将依赖关系（TaskNode对象）转换为其name_id列表
        dependencies_list = [dep.name_id for dep in self.dependencies]
        # 调用辅助函数处理 task.result
        processed_result = self._process_result(self.result)
        
        return {
            "name_id": self.name_id,
            "status": self.status,
            "retry_times": self.retry_times,
            "result": processed_result,
            "dependencies": dependencies_list, 
        }
    
    # 使用静态方法的中介量，保证外部可序列化对应方法（即不传入类实例，而是类与其构造参数）
    @staticmethod
    def static_execute(task_class, resource_conf, name_id, params, glob_params):
        task = task_class(name_id=name_id, params=params, glob_params=glob_params)
        return task._execute(resource_conf)

    def _execute(self, resource_conf):
        try:
            result = self._custom_task(resource_conf, self.params)
        except Exception as e:
            result = {"status": "failed", "data": None, "error": str(e)} 
            
        if self.params.get("save", 0) > 0:
            save_type = self.params.get("save", 0)
            
            # 根据 save_type 的值执行不同的存储操作
            if save_type == 1:
                # 添加 DB 存储的逻辑
                self._save_to_db(resource_conf, result.get("data", None), self.params.get("target_db", ""))
            elif save_type == 2:
                self._save_to_file(result.get("data", None), self.glob_params.get("save_path", ""))
            else:
                # 处理其他未知 save_type 的情况
                raise ValueError(f"Save type: {save_type} not recognized.")
            
        if self.params.get("register", False):
            next_pipeline = self._register_pipeline(result.get("data", None))
            result["next_pipelines"] = next_pipeline
            
        return result
    
    # 内部函数，需被子类重写以实现具体的任务逻辑
    def _custom_task(self, resource_conf, params):
        raise NotImplementedError()
    
    # ---------------------------- 以下为tasknode除核心执行逻辑以外可能附带的运行逻辑，可被重写 ----------------------------
    # ---------------------------- 数据存储部分 ----------------------------
    def _save_to_db(self, resource_conf, result, target_db):
        if result is not None and not result.empty:
            db_manager = ResourceManager.create("postgres", resource_conf)
            session = db_manager.get_session()
            insert_dataframe_to_table(result, target_db, session, True)
        else:
            self.logger.warning("No data to save.")
        
    def _save_to_file(self, result, save_path):
        # 检查是否有数据
        if result is None:
            self.logger.warning("No data to save.")
            return

        # 如果保存路径不存在，则创建它
        if not os.path.exists(save_path):
            os.makedirs(save_path)
            self.logger.info(f"Created directory: {save_path}")

        # 判断数据类型并保存
        if isinstance(result, pd.DataFrame):
            # 处理 DataFrame 类型，保存为 CSV
            if not result.empty:
                output_path = os.path.join(save_path, f"{self.name_id}.csv")
                result.to_csv(output_path, encoding="utf-8-sig", index=False)
                self.logger.info(f"DataFrame saved to {output_path}")
            else:
                self.logger.warning("The DataFrame is empty, nothing to save.")
        else:
            # 处理非 DataFrame 类型，将其转换为字符串并保存为 TXT
            output_path = os.path.join(save_path, f"{self.name_id}.txt")
            try:
                with open(output_path, 'w', encoding='utf-8') as f:
                    f.write(str(result))
                self.logger.info(f"Data saved to {output_path}")
            except Exception as e:
                self.logger.error(f"Failed to save data to {output_path}: {e}")
    
    # ---------------------------- 动态注册部分（需重写） ----------------------------
    def _register_pipeline(self, result):
        """该方法需要生成pipeline的基础配置，以便注册器能正确生成pipeline初始化所需配置"""
        self.logger.info(f"Dynamically add pipeline.")
        
    def _register_task(self, result):
        """该方法需要生成tasknode的基础配置，以便注册器能正确生成tasknode初始化所需配置"""
        self.logger.info(f"Dynamically add task.")
    
        
