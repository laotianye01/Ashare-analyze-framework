import pandas as pd
import time
from bs4 import BeautifulSoup
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from core.resource_manager import ResourceManager
from selenium.common.exceptions import NoSuchElementException
from core.taskNode import TaskNode

from utils.html_parser import extract_main_content
from utils.database_utils import *
from utils.dataframe_utils import *
import akshare as ak


class FetchStockNews(TaskNode):
    def _custom_task(self, resource_config, params=None):
        """
        获取个股新闻（通过 akshare）
        """
        try:
            code = params.get("stock_code")
            data = ak.stock_news_em(code)
            data = preprocess_stock_news(data, code)
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

class FetchBingNews(TaskNode):
    def custom_task(self, resource_config, params=None):
        """
        使用 Bing 搜索新闻，并根据 URL 白名单访问页面，保存 HTML 内容。
        params:
            query: 搜索关键词
            max_results: 返回的最大条目数
            whitelist: 包含的网站域名，如 ["finance.sina.com.cn", "cn.reuters.com"]
        """
        try:
            query = params.get("query")
            max_results = params.get("max_results", 15)
            whitelist = params.get("whitelist", [])  # URL 白名单

            driver = ResourceManager.create("searcher", resource_config)
            if driver is None:
                raise ValueError("Bing WebDriver is not initialized in resource_manager")

            driver.get("https://www.bing.com")

            # 最多等待10秒直到搜索框出现
            search_box = WebDriverWait(driver.driver, 10).until(
                EC.presence_of_element_located((By.ID, "sb_form_q"))
            )
            
            search_box.clear()
            search_box.send_keys(query)
            search_box.send_keys(Keys.RETURN)
            time.sleep(2)
            
            # elements 是一组 WebElement 对象的引用，无法被复制，当driver重新执行搜索时其内容会被改变
            elements = driver.driver.find_elements(By.CSS_SELECTOR, "li.b_algo")
            news_links = []

            for elem in elements[:max_results]:
                try:
                    title_elem = elem.find_element(By.CSS_SELECTOR, "h2 a")
                    title = title_elem.text
                    link = title_elem.get_attribute("href")

                    snippet = ""
                    try:
                        snippet_elem = elem.find_element(By.CSS_SELECTOR, "div.b_caption p")
                        snippet = snippet_elem.text
                    except NoSuchElementException:
                        pass

                    # 检查白名单
                    if whitelist and not any(domain in link for domain in whitelist):
                        continue

                    news_links.append((title, link, snippet))

                except Exception as e:
                    print(f"[跳过一条结果] Error (预处理): {e}")
                    continue

            # 再逐一访问每个页面
            results = []
            for title, link, snippet in news_links:
                try:
                    driver.get(link)
                    time.sleep(2)
                    html = driver.driver.page_source
                    content_info = extract_main_content(html, link)
                    results.append({
                        "title": content_info["title"] or title,
                        "snippet": snippet,
                        "content": content_info["content"],
                        "time": content_info["time"],
                    })

                except Exception as e:
                    print(f"[跳过一条结果] Error (访问页面): {e}")
                    continue
                
            df = pd.DataFrame(results)
            return {"status": "success", "data": df, "error": None}
                
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

class FetchDailyNews(TaskNode):
    def _custom_task(self, resource_config, params=None):
        """
        获取东方财富财经日评
        """
        try:
            driver = ResourceManager.create("searcher", resource_config)
            if driver is None:
                raise ValueError("WebDriver is not initialized in resource_manager")

            url = "https://stock.eastmoney.com/a/czpnc.html"
            driver.get(url)
            time.sleep(1)

            news_link_elem = driver.find_element(By.CSS_SELECTOR, "#newsTr1 > div.image > a")
            news_url = news_link_elem.get_attribute('href')
            driver.get(news_url)

            WebDriverWait(driver, 10).until(
                EC.presence_of_element_located((By.ID, "ContentBody"))
            )

            page_source = driver.page_source
            soup = BeautifulSoup(page_source, 'html.parser')
            content_body = soup.select_one("#ContentBody")

            if not content_body:
                return {"status": "success", "data": pd.DataFrame(columns=["序号", "内容"]), "error": None}

            paragraphs = content_body.select("p:nth-child(n+5)")
            data = [{"序号": idx, "内容": p.get_text(strip=True)}
                    for idx, p in enumerate(paragraphs, start=1) if p.get_text(strip=True)]

            df = pd.DataFrame(data)
            return {"status": "success", "data": df, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
