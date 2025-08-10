import os
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium import webdriver

from core.logger import get_logger

logger = get_logger("ResourceManager")


# 浏览器资源初始化  
class BingSearcher:
    def __init__(self, driver_path: str):
        service = Service(executable_path=driver_path, log_path=os.devnull)
        options = Options()
        options.add_argument("--headless")  
        options.add_argument("--disable-gpu")
        options.add_argument("--no-sandbox")
        options.add_argument("--log-level=3")
        options.add_experimental_option('excludeSwitches', ['enable-logging'])
        self.driver = webdriver.Chrome(service=service, options=options)

    def close(self):
        """关闭浏览器"""
        self.driver.quit()

    # 将 driver 的 get 方法暴露出来
    def get(self, url: str):
        return self.driver.get(url)

    # 如果你还需要支持其他方法，比如 find_element 等，可以继续包装
    def find_element(self, *args, **kwargs):
        return self.driver.find_element(*args, **kwargs)

    def find_elements(self, *args, **kwargs):
        return self.driver.find_elements(*args, **kwargs)

    # 也可以实现 __getattr__ 自动转发所有 driver 的方法（更通用）
    def __getattr__(self, item):
        return getattr(self.driver, item)