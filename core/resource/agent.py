from langchain.llms.base import LLM
from langchain.memory import ConversationBufferMemory
from langchain.chains.summarize import load_summarize_chain
from langchain.docstore.document import Document
from langchain.text_splitter import CharacterTextSplitter
from pydantic import PrivateAttr
from pydantic import PrivateAttr
from typing import Dict, Any, Optional, List
from openai import OpenAI
from duckduckgo_search import DDGS
from langchain.prompts import PromptTemplate


'''
自定义平台的大模型的接口类，其集成短期记忆、长期记忆、联网搜索功能
以下类由于继承了longchain的base类，所以需要使用如下的方式进行初始化？？
'''
class AILLM(LLM):
    _client: OpenAI = PrivateAttr()
    _search_tool: Optional[DDGS] = PrivateAttr(default=None)

    # 参数
    api_key: str
    endpoint_url: str
    model_name: str
    temperature: float
    max_tokens: int

    use_short_term_memory: bool = False
    short_term_memory: Optional[ConversationBufferMemory] = None

    stock_code: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._client = OpenAI(
            api_key=self.api_key,
            base_url=self.endpoint_url,
        )

        if self.use_short_term_memory:
            self.short_term_memory = ConversationBufferMemory()

    @property
    def _llm_type(self) -> str:
        return "ali_llm"

    def _web_search(self, query: str, max_results: int) -> str:
        if self._search_tool is None:
            return ""
        try:
            result = self._search_tool.text(query, max_results)
            return result
        except Exception as e:
            return f"联网搜索失败: {e}"

    def _call(self, prompt: str, stop: Optional[List[str]] = None) -> str:
        """
        这个方法是 LangChain LLM 基类的核心，因此其签名需要与基类保持一致。
        我们假设 LangChain 的 chain 已经构建好了完整的 prompt，
        并将其作为唯一的 prompt 参数传递进来。
        """
        
        try:
            completion = self._client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {
                        'role': 'user',
                        'content': prompt
                    }
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )
            
            print(completion.usage)

            output_data = completion.model_dump()
            output_text = output_data["choices"][0]["message"]["content"]

            return output_text

        except Exception as e:
            raise RuntimeError(f"调用大模型API失败: {e}")

    def chat(self, prompt: str, context: str) -> str:
        """
        这个方法将用户输入的 prompt 和 context 组合成一个完整的 prompt，
        然后调用 _call 方法。
        """
        # 将用户的 prompt 和 context 组合成一个完整的字符串
        full_prompt = f"{context}\n\n{prompt}"
        res = self._call(full_prompt)
        return {"status": "success", "data": res, "error": None}
    
    def refine_chat(self, user_prompt: str, long_context, initial_prompt: Optional[PromptTemplate] = None, refine_prompt: Optional[PromptTemplate] = None) -> Dict[str, Any]:
        """
        使用 LangChain 的 Refine 链处理超长文本输入。

        :param user_prompt: 用户的原始问题。
        :param long_context: 需要处理的超长文本内容。
        :return: 包含精炼后答案的字典。
        """
        # 根据 long_context 的类型来创建 LangChain Document 列表
        docs = []
        if isinstance(long_context, str):
            # 如果是字符串，则进行切分
            text_splitter = CharacterTextSplitter(chunk_size=self.max_tokens, chunk_overlap=0)
            texts = text_splitter.split_text(long_context)
            docs = [Document(page_content=t) for t in texts]
        elif isinstance(long_context, dict):
            # 如果是字典，则为每个键值对创建一个 Document
            for data_name, data_content in long_context.items():
                if not isinstance(data_content, str):
                    raise TypeError(f"字典中的值必须为字符串，但 {data_name} 的值为 {type(data_content)}")
                
                # 创建 Document，并将原始标签作为元数据
                docs.append(Document(page_content=data_content, metadata={"source": data_name}))
        else:
            raise TypeError("long_context 必须是字符串 (str) 或字典 (Dict[str, str]) 类型")

        # 3. 加载 Refine 链
        refine_chain = load_summarize_chain(
            self,
            chain_type="refine",
            verbose=False, # 设置为 True 可查看详细处理过程
            question_prompt=initial_prompt,
            refine_prompt=refine_prompt,
        )

        # 4. 执行链并获取结果
        try:
            val = refine_chain.llm_chain.llm.max_tokens
            print(val)
            refined_result = refine_chain.run(input_documents=docs, question=user_prompt)
            # Refine 链的运行结果即为最终答案，我们将其保存到短期记忆
            if self.short_term_memory is not None:
                self.short_term_memory.save_context(
                    {"input": initial_prompt},
                    {"output": refined_result}
                )
            return {"status": "success", "data": refined_result, "error": None}
        except Exception as e:
            raise RuntimeError(f"使用 Refine 链处理长文本失败: {e}")

    def clear_memory(self):
        if self.short_term_memory is not None:
            self.short_term_memory.clear()
