import json
import os
import logging

# 假设您已经定义了 logger
logger = logging.getLogger("WebApp")

class PipelineConfigManager:
    """
    负责加载、存储和管理所有 Pipeline 配置文件的元数据。
    """
    def __init__(self, folder_path):
        self.folder_path = folder_path
        self.configs_meta = self._load_configs_meta()

    def _load_configs_meta(self):
        """
        扫描指定文件夹，获取所有 .json 配置文件列表及其元数据。
        返回一个列表，每个元素是一个字典，包含 'name' (文件名，不含扩展名) 和 'path' (完整路径)。
        """
        configs_meta = []
        if not os.path.exists(self.folder_path):
            logger.warning(f"Pipeline config folder '{self.folder_path}' does not exist.")
            return configs_meta
        
        for filename in os.listdir(self.folder_path):
            if filename.endswith('.json'):
                full_path = os.path.join(self.folder_path, filename)
                config_name = os.path.splitext(filename)[0] # 获取不带扩展名的文件名
                configs_meta.append({
                    'name': config_name,
                    'path': full_path
                })
        return configs_meta

    def get_all_configs_meta(self):
        """
        返回所有已加载的配置元数据列表。
        """
        return self.configs_meta

    def _get_config_path_by_name(self, config_name):
        """
        根据配置名称查找其完整路径。
        如果找到，则返回路径字符串；否则，返回 None。
        """
        for config_meta in self.configs_meta:
            if config_meta['name'] == config_name:
                return config_meta['path']
        return None
    
    def _convert_pipeline_to_update_format(self, pipeline_config: dict):
        """
        将原始的 pipeline 配置转换为 scheduler 可处理的格式。
        """
        # 提取需要作为独立更新的 pipeline 参数
        pipeline_params = {
            "multi_launch": pipeline_config.get("multi_launch"),
            "keep_after_finfish": pipeline_config.get("keep_after_finfish"),
            "daily_launch_times": pipeline_config.get("daily_launch_times"),
            "launch_interval_minutes": pipeline_config.get("launch_interval_minutes"),
            "retry_interval": pipeline_config.get("retry_interval"),
            "max_retry_times": pipeline_config.get("max_retry_times"),
            "save_path": pipeline_config.get("save_path")
        }
        
        # 移除 None 值的参数，使格式更简洁
        cleaned_params = {k: v for k, v in pipeline_params.items() if v is not None}

        if not cleaned_params:
            return None
            
        return {
            "type": "pipeline",
            "name": pipeline_config.get("name"),
            "params": cleaned_params
        }

    def _convert_task_to_update_format(self, pipeline_name: str, task_config: dict):
        """
        将原始的 task 配置转换为 scheduler 可处理的格式，将所有参数置于同一层级。
        """
        final_params = {}

        # 获取并合并 params 字典中的所有键值对
        task_specific_params = task_config.get("params", {})
        if task_specific_params:
            final_params.update(task_specific_params)

        # 获取并添加 retry_interval 参数
        retry_interval = task_config.get("retry_interval")
        if retry_interval is not None:
            final_params["retry_interval"] = retry_interval

        # 如果没有可用的参数，则返回 None
        if not final_params:
            return None

        return {
            "type": "task",
            "pipeline_name": pipeline_name,
            "task_name": task_config.get("name"),
            "params": final_params
        }

    def get_config_for_update(self, config_name: str):
        """
        根据配置名称，读取并将其内容转换为 scheduler command_listener 可处理的格式。
        返回一个字典，包含该 pipeline 及其所有 task 的更新格式列表。
        
        参数:
            config_name: 字符串，对应 Pipeline 配置文件的名称。
        
        返回:
            一个字典，如果找到配置，则包含 "updates" 列表；否则返回 None。
        """
        # 1. 根据名称查找配置文件路径
        config_path = self._get_config_path_by_name(config_name)
        if not config_path:
            logger.warning(f"Configuration file '{config_name}' not found.")
            return None

        updates_list = []
        try:
            # 2. 打开并加载原始 JSON 文件
            with open(config_path, 'r', encoding='utf-8') as f:
                pipeline_config = json.load(f)
            
            # 3. 转换 pipeline 配置
            pipeline_update = self._convert_pipeline_to_update_format(pipeline_config)
            if pipeline_update:
                updates_list.append(pipeline_update)
            
            # 4. 转换所有 task 配置
            for task_conf in pipeline_config.get("tasks", []):
                task_update = self._convert_task_to_update_format(pipeline_config["name"], task_conf)
                if task_update:
                    updates_list.append(task_update)
                    
        except FileNotFoundError:
            logger.error(f"Config file not found: {config_path}")
            return None
        except json.JSONDecodeError:
            logger.error(f"Error decoding JSON file: {config_path}")
            return None
        except Exception as e:
            logger.error(f"An error occurred while processing {config_path}: {e}")
            return None
            
        return {"updates": updates_list}
