# routes/config_api.py
import json
from flask import Blueprint, abort, jsonify
from web.glob_resource import config_manager

# 创建一个名为 'config_api' 的蓝图
# 所有路由的URL前缀将为 /configs
config_bp = Blueprint('config_api', __name__, url_prefix='/configs')

@config_bp.route('/', methods=['GET'])
def list_available_pipeline_configs():
    """
    返回所有可用的流水线配置文件列表。
    这些数据在应用启动时被加载到 app.config['PIPELINE_OPTIONS'] 中。
    """
    # current_app 允许在蓝图内部访问 Flask 应用实例
    return jsonify(config_manager.get_all_configs_meta())

@config_bp.route('/<string:config_name>', methods=['GET'])
def get_pipeline_config_by_name(config_name):
    """
    根据给定的配置名称，返回该 Pipeline 的完整配置，格式化为 scheduler 可更新的格式。
    
    参数:
        config_name: 字符串，对应 Pipeline 配置文件的名称。
        
    返回:
        JSON 格式的 Pipeline 配置，如果找不到则返回 404 错误。
    """
    # 直接调用 config_manager 中新的方法来获取重构后的配置
    response_data = config_manager.get_config_for_update(config_name)

    # 错误处理
    if response_data is None:
        abort(404, description=f"Configuration file '{config_name}' not found or could not be processed.")
    if not response_data.get("updates"):
        abort(404, description=f"Configuration file '{config_name}' contains no updatable parameters.")

    return jsonify(response_data)