from langchain.llms.base import LLM
from langchain.memory import ConversationBufferMemory
from pydantic import PrivateAttr
from typing import Optional, List
from openai import OpenAI
from duckduckgo_search import DDGS


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

    def _call(self, prompt: str, context: str, stop: Optional[List[str]] = None) -> str:
        if self.short_term_memory is not None:
            context += self.short_term_memory.buffer

        try:
            completion = self._client.chat.completions.create(
                model=self.model_name,
                messages=[
                    {
                        'role': 'system',
                        'content': prompt
                    },
                    {
                        'role': 'user',
                        'content': context
                    }
                ],
                temperature=self.temperature,
                max_tokens=self.max_tokens
            )

            output_data = completion.model_dump()
            output_text = output_data["choices"][0]["message"]["content"]

            if self.short_term_memory is not None:
                self.short_term_memory.save_context({"input": prompt}, {"output": output_text})

            return output_text

        except Exception as e:
            raise RuntimeError(f"调用大模型API失败: {e}")
        

    def chat(self, prompt: str, context: str) -> str:
        res = self._call(prompt, context)
        return {"status": "success", "data": res, "error": None}

    def clear_memory(self):
        if self.short_term_memory is not None:
            self.short_term_memory.clear()