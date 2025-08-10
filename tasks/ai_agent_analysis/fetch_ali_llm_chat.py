# tasks/fetch_ali_llm_chat.py
from core.resource_manager import ResourceManager
from utils.embedding_module import embed_text 
from core.taskNode import TaskNode

class FetchAILLMChat(TaskNode):
    def _custom_task(self, resource_config, params=None):
        """
        调用 AliLLM 进行带记忆对话
        参数 params 要求：
        - user_input: 用户输入内容（必填）
        - all_data: 模型需要的所有prompt，为一个字典，其中包含net_search_prompt + related_data
        - new_memory: 是否每次任务创建新短期记忆（默认False）
        """
        try:
            if params is None:
                raise ValueError("params must not be None")
            
            ali_llm = ResourceManager.create("LLM", resource_config)
            if ali_llm is None:
                raise ValueError("AliLLM instance not found in resource_manager")
            
            user_prompt = params.get("user_prompt")
            if not user_prompt:
                raise ValueError("user_prompt is required in params")
            
            # 通过输入股票代码来初始化长期记忆数据库
            # TODO: 此处查询需要通过关键词在长期记忆中进行查询，可能需要在此处构建一个关键词库。
            llmdb_manager = ResourceManager.create("LLMdatabase", resource_config)
            long_term_memory = None
            vector_database_prompt = ""
            if params.get("use_long_term_memory"):
                idx = params.get("vector_database_index")
                if idx == 6:
                    long_term_memory = llmdb_manager.get_vector_database(idx)
                    
                history_items = long_term_memory.get_history("history")
                history_text = "\n".join(f"[Memory] {item}" for item in history_items)
                vector_database_prompt += history_text
                
            context = ""
            all_data = params.get("all_data", {})
            # 添加联网搜索相关的内容
            for data_name, data in all_data.items():
                if data_name is not None and data is not None:
                    # 确保将所有内容都转换为字符串
                    context += f"{str(data_name)}相关内容为：{str(data)}\n"
            
            response = ali_llm.chat(user_prompt, context)
            
            # 保存到长期记忆
            # TODO: 需要修改插入语句逻辑
            if params.get("use_long_term_memory") and long_term_memory is not None and response.get("status") == "success":
                input_embedding = embed_text(context)
                output_embedding = embed_text(response.get("data"))

                long_term_memory.insert(
                    embeddings=[input_embedding, output_embedding],
                    texts=[context, response.get("data")],
                    metadatas=[{"type": "input"}, {"type": "output"}]
                )
            
            return {"status": "success", "data": response, "error": None}

        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
