import trafilatura

def extract_main_content(html_str, url=""):
    try:
        downloaded = trafilatura.extract(html_str, include_comments=False, include_tables=False, url=url)
        if not downloaded:
            raise ValueError("正文提取失败，内容为空")

        result = trafilatura.extract(html_str, output_format='json', url=url)
        if result:
            import json
            data = json.loads(result)
            return {
                "title": data.get("title", ""),
                "content": data.get("text", ""),
                "time": data.get("date", None)
            }
        else:
            return {
                "title": "",
                "content": "",
                "time": None
            }
    except Exception as e:
        print(f"[正文提取失败] {e}")
        return {
            "title": "",
            "content": "",
            "time": None
        }
