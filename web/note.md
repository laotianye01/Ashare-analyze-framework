**用户端架构**
* index.html：页面的骨架，定义了页面上所有元素的布局
  * 使用<link rel="stylesheet" href="styles.css"> 标签链接了 styles.css，用于渲染界面的布局
  * 使用<script src="script.js"></script> 标签链接了 script.js 文件，用于触发对应的js脚本与后端交互
  * 使用id="pipeline-view" 唯一标识符标注页面上的标签，使得js能够找到对应元素的围追

* styles.css：页面的外观，定义页面上每个元素的颜色、字体、大小、布局等
  * 其内部定义了html中每一个标签的样式与布局等信息(如header h1与html中的<h1>标签相互对应)

* script.js：页面的大脑，负责处理所有动态行为。document.getElementById可通过id获取html中对应的元素引用，对其修改可改变客户端html的显示（找到元素再进行事件注册，这可以成功解偶html与js）
  * 与后端通信：定期向你的 Flask 后端 API 发送 fetch 请求
  * 数据处理：解析从后端收到的 JSON 数据
  * 界面渲染：根据数据内容，动态地创建或修改 HTML 元素，将数据显示在页面上
  * 事件监听：监听用户的操作，比如点击调度器列表项、点击刷新按钮等，并根据这些操作执行相应的函数