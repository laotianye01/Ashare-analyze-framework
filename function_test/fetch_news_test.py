import csv
from bs4 import BeautifulSoup
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
import time
from utils import akshare as ak


class BingSearcher:
    def __init__(self, driver_path: str):
        service = Service(executable_path=driver_path)
        options = Options()
        options.add_argument("--headless")  
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument('--log-level=2')
        self.driver = webdriver.Chrome(service=service, options=options)

    def search(self, query: str, max_results: int = 15) -> list:
        """在bing搜索并返回前max_results个搜索结果：标题、链接、摘要"""
        self.driver.get("https://www.bing.com")
        time.sleep(1)  # 等页面加载

        search_box = self.driver.find_element(By.ID, "sb_form_q")  # Bing搜索框id
        search_box.send_keys(query)
        search_box.send_keys(Keys.RETURN)

        time.sleep(2)  # 等搜索结果加载

        results = []
        elements = self.driver.find_elements(By.CSS_SELECTOR, "li.b_algo")  # Bing每个搜索结果的容器

        for elem in elements[:max_results]:
            try:
                title_elem = elem.find_element(By.CSS_SELECTOR, "h2 a")
                title = title_elem.text
                link = title_elem.get_attribute("href")
                
                # 尝试获取摘要
                try:
                    snippet_elem = elem.find_element(By.CSS_SELECTOR, "div.b_caption p")
                    snippet = snippet_elem.text
                except:
                    snippet = "摘要信息未找到"

                results.append({"title": title, "link": link, "snippet": snippet})

            except Exception as e:
                print(f"提取某条结果出错: {str(e)}")
                continue

        return results

    def close(self):
        """关闭浏览器"""
        self.driver.quit()

def fetch_stock_news(code: str):
    return ak.stock_news_em(code)

def obtain_daily_news():
    # 设置 WebDriver 路径
    driver_path = './tasks/news_data/chromedriver/chromedriver.exe'  # 替换为你的 chromedriver 路径

    # 创建 Service 对象
    service = Service(executable_path=driver_path)

    # 创建 Options 对象（可选）
    options = Options()
    options.add_argument("--headless")  # 无头模式，不加载chrome界面

    # 创建 WebDriver 实例
    driver = webdriver.Chrome(service=service, options=options)

    try:
        # 打开目标网页
        url = "https://stock.eastmoney.com/a/czpnc.html"
        driver.get(url)

        # 等待页面加载
        time.sleep(1)  # 等待 1 秒，确保页面完全加载

        # 查找 #newsTr0 元素
        news_tr0 = driver.find_element(By.CSS_SELECTOR, "#newsTr0")
        if news_tr0:
            # 查找 #newsTr1 > div.image > a 元素
            news_tr1_link = driver.find_element(By.CSS_SELECTOR, "#newsTr1 > div.image > a")
            if news_tr1_link:
                link_href = news_tr1_link.get_attribute('href')
                print(f"Found link: {link_href}")

                # 访问获取到的链接
                driver.get(link_href)

                # 使用显式等待确保页面完全加载
                try:
                    WebDriverWait(driver, 10).until(
                        EC.presence_of_element_located((By.ID, "ContentBody"))
                    )
                except Exception as e:
                    print(f"Timeout waiting for page to load: {e}")
                    return

                # 获取目标页面的 HTML 内容
                daily_news = driver.page_source

                # 使用 BeautifulSoup 解析 HTML
                soup = BeautifulSoup(daily_news, 'html.parser')

                # 查找 #ContentBody 元素
                content_body = soup.select_one("#ContentBody")
                if not content_body:
                    print("Element #ContentBody not found.")
                    return

                # 提取从第 5 个子元素开始的所有 <p> 元素
                paragraphs = content_body.select("p:nth-child(n+5)")

                # 准备保存到 CSV 文件的数据
                data = []

                # 遍历每个 <p> 元素，提取文本并拼接
                for idx, p in enumerate(paragraphs, start=1):
                    text = p.get_text(strip=True)
                    if text:
                        data.append([idx, text])

                # 保存到 CSV 文件
                csv_file = "./cache/daily_news.csv"
                with open(csv_file, mode='w', newline='', encoding='utf-8') as file:
                    writer = csv.writer(file)
                    writer.writerow(["序号", "内容"])  # 写入表头
                    writer.writerows(data)  # 写入数据

                print(f"Data saved to {csv_file}")

                # # 保存目标页面的 HTML 内容到文件
                # with open("daily_news.html", "w", encoding="utf-8") as file:
                #     file.write(daily_news)

                print("Target page HTML saved to 'daily_news.html'.")
            else:
                print("Element #newsTr1 > div.image > a not found.")
        else:
            print("Element #newsTr0 not found.")
    finally:
        # 关闭 WebDriver
        driver.quit()
        