from langchain.llms.base import LLM
from langchain.memory import ConversationBufferMemory
from langchain.memory.vectorstore import VectorStoreRetrieverMemory
from pydantic import PrivateAttr
from typing import Optional, List
from openai import OpenAI
from duckduckgo_search import DDGS

from core.resource_manager import StockMemoryManager, VectorDatabase
from utils.embedding_module import embed_text


class AliLLM(LLM):
    """
    自定义接入阿里百炼（dashscope）平台的大模型，集成短期记忆、长期记忆、联网搜索功能
    """

    _client: OpenAI = PrivateAttr()
    _search_tool: Optional[DDGS] = PrivateAttr(default=None)

    # 参数
    StockMemoryManager: StockMemoryManager
    api_key: str
    endpoint_url: str = ""
    model_name: str = "qwen-plus"
    temperature: float = 0.7
    max_tokens: int = 2048

    use_short_term_memory: bool = True
    use_long_term_memory: bool = False
    use_web_search: bool = True

    short_term_memory: Optional[ConversationBufferMemory] = None
    long_term_memory: Optional[VectorDatabase] = None

    stock_code: Optional[str] = None

    def __init__(self, **kwargs):
        super().__init__(**kwargs)

        self._client = OpenAI(
            api_key=self.api_key,
            base_url=self.endpoint_url,
        )

        if self.use_short_term_memory:
            self.short_term_memory = ConversationBufferMemory()

        if self.use_web_search:
            self._search_tool = DDGS()

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

    def _call(self, prompt: str, net_search_prompt: str, vector_database_index: str, stop: Optional[List[str]] = None) -> str:
        context = ""

        if self.short_term_memory is not None:
            context += self.short_term_memory.buffer

        if self.use_web_search and net_search_prompt[:3] == 'yes':
            search_result = self._web_search(net_search_prompt[3:], 10)
            context += f"\n[网络搜索结果]: {search_result}\n"
            print("搜索结果为：\n", search_result)
        
        # 通过输入股票代码来初始化长期记忆数据库    ALTER USER your_app_user WITH SUPERUSER
        # TODO: 此处查询需要通过关键词在长期记忆中进行查询，可能需要在此处构建一个关键词库
        if self.use_long_term_memory:
            if len(vector_database_index) == 6:
                self.long_term_memory = self.StockMemoryManager.get_vector_database(vector_database_index)
                print("更新当前的向量数据库")
                
            history_items = self.long_term_memory.get_history("history")
            history_text = "\n".join(f"[Memory] {item}" for item in history_items)
            context = history_text + "\n" + context
            print("从已有的向量数据库中获取信息")

        full_prompt = f"{context}\n用户: {prompt}\n助手:"

        try:
            completion = self._client.chat.completions.create(
                model=self.model_name,
                messages=[{"role": "user", "content": full_prompt}],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )

            output_data = completion.model_dump()
            output_text = output_data["choices"][0]["message"]["content"]

            if self.short_term_memory is not None:
                self.short_term_memory.save_context({"input": prompt}, {"output": output_text})

            # 保存到长期记忆
            # TODO: 需要修改插入语句逻辑
            if self.use_long_term_memory and self.long_term_memory:
                input_embedding = embed_text(prompt)
                output_embedding = embed_text(output_text)

                self.long_term_memory.insert(
                    embeddings=[input_embedding, output_embedding],
                    texts=[prompt, output_text],
                    metadatas=[{"type": "input"}, {"type": "output"}]
                )

            return output_text

        except Exception as e:
            raise RuntimeError(f"调用阿里大模型API失败: {e}")

    def chat(self, user_input: str, net_search_prompt: str, vector_database_index: str) -> str:
        return self._call(user_input, net_search_prompt, vector_database_index)

    def clear_memory(self):
        if self.short_term_memory is not None:
            self.short_term_memory.clear()
        if self.long_term_memory is not None:
            self.long_term_memory.clear()



if __name__ == "__main__":
    memoryManager = StockMemoryManager(
        dim=384,
        db_params={
            "dbname": "stock_db",
            "user": "",
            "password": "",
            "host": "localhost",
            "port": "5432",
        }
    )
    
    ali_llm = AliLLM(
        api_key="",
        endpoint_url="",
        use_short_term_memory=True,  # 开启短期记忆
        use_long_term_memory=True,   # 开启长期记忆
        temperature=0.5,
        StockMemoryManager=memoryManager,
    )
    
    while True:
        user_input = input("你想问什么？> ")

        net_search_prompt = input("你需要在网上搜索什么？> ")
        long_term_memory_for_stock = input("你需要哪个股票的长期数据库？> ")
        response = ali_llm.chat(user_input, net_search_prompt, long_term_memory_for_stock)
        print("AI回答:", response)

