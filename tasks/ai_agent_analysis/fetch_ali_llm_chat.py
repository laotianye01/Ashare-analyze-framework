# tasks/fetch_ali_llm_chat.py
from core.resource_manager import ResourceManager
from utils.embedding_module import *
from core.taskNode import TaskNode
from langchain.prompts import PromptTemplate

class FetchAILLMChat(TaskNode):
    def _get_analyze_prompt(self):
        # 自定义 Refine 链的 Prompt
        initial_prompt_template = """
        您是一位专业的投资分析师。任务是根据以下文档和用户问题，对其中涉及的核心财务数据、指标变化和潜在趋势进行**详细分析**，并给出**一个全面且结构化的初步报告**。请不仅限于列举数据，还要阐述这些数据背后的含义。
        
        用户问题: {question}
        
        文档内容:
        {text}
        
        请确保更新后的报告不少于 2000 字，尽可能详细地展开所有数据变化的背景、原因、潜在影响，并提供详细的推演分析。
        """
        # 用于后续精炼的 Prompt
        refine_prompt_template = """
        您是同一位专业的投资分析师。现在您需要基于新的文档内容，**深入补充和精炼**之前的报告。请将新文档中的数据与旧数据进行**对比分析**，找出新的趋势或异常点，并更新您的报告。最终的回答必须是一个整合了所有文档信息、逻辑连贯、且分析详尽的完整报告。
        
        用户问题: {question}
        
        之前的回答:
        {existing_answer}

        新的文档内容:
        {text}
        
        请确保更新后的报告不少于 2000 字，尽可能详细地展开所有数据变化的背景、原因、潜在影响，并提供详细的推演分析。
        """
        
        initial_prompt = PromptTemplate(template=initial_prompt_template, input_variables=["text", "question"])
        refine_prompt = PromptTemplate(template=refine_prompt_template, input_variables=["existing_answer", "text", "question"])
        
        return initial_prompt, refine_prompt
        
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
                
                
            all_data = params.get("all_data", {})
            max_model_tokens = ali_llm.max_tokens / 2
            processed_data_parts = {}
            # 逐个检查 all_data 中的每一项，对过长项进行总结
            for data_name, data in all_data.items():
                if data_name is not None and data is not None:
                    data_str = str(data)
                    data_tokens = get_token_count(data_str)
                    
                    # 为确保总结时不会超出 token 限制，留出一定余量
                    summary_threshold = max_model_tokens - 200
                    
                    if data_tokens > summary_threshold:
                        # 阶段一：调用 LLM 对该部分内容进行总结
                        summary_prompt = f"请作为专家，总结以下关于【{data_name}】的内容，提炼出关键信息和核心要点。"
                        summary_response = ali_llm.chat(user_prompt=summary_prompt, context=data_str)
                        
                        if summary_response and summary_response.get("status") == "success":
                            summary_text = summary_response.get("data")
                            processed_data_parts[data_name] = summary_text
                        else:
                            raise RuntimeError(f"处理 {data_name} 的长文本摘要失败，无法进行后续分析。")
                    else:
                        # 如果内容长度正常，则直接保留
                        processed_data_parts[data_name] = data_str
            
            init_prompt, refine_prompt = self._get_analyze_prompt()
            response = ali_llm.refine_chat(user_prompt, processed_data_parts, init_prompt, refine_prompt)
            # 保存到长期记忆
            # TODO: 需要修改插入语句逻辑
            if params.get("use_long_term_memory") and long_term_memory is not None and response.get("status") == "success":
                input_embedding = embed_text(str(processed_data_parts))
                output_embedding = embed_text(response.get("data"))

                long_term_memory.insert(
                    embeddings=[input_embedding, output_embedding],
                    texts=[str(processed_data_parts), response.get("data")],
                    metadatas=[{"type": "input"}, {"type": "output"}]
                )
            
            return {"status": "success", "data": response, "error": None}

        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
