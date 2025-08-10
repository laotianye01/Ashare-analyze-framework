// util.js


// 构建 Mermaid 流程图代码
export function buildMermaidCode(pipelineData) {
    const tasks = pipelineData.tasks;
    let code = 'graph TD;\n';
    
    // 定义所有任务节点
    for (const taskId in tasks) {
        const task = tasks[taskId];
        let statusClass;
        switch(task.status) {
            case 'success': statusClass = 'success'; break;
            case 'failed': statusClass = 'failed'; break;
            case 'running': statusClass = 'running'; break;
            default: statusClass = 'pending'; break;
        }
        // 使用更安全的 Mermaid 语法，并添加点击事件的占位符
        code += `    ${taskId}["${taskId}"]:::${statusClass};\n`;
    }

    // 添加依赖关系
    for (const taskId in tasks) {
        const task = tasks[taskId];
        // 确保 dependencies 是一个数组，如果不存在则默认为空数组
        const dependencies = task.dependencies || [];
        
        // 使用更安全的 for...of 循环来遍历依赖项
        for (const depId of dependencies) {
            code += `    ${depId}-->${taskId};\n`;
        }
    }
    
    // 定义样式，这里使用 Mermaid 官方文档的推荐格式
    code += 'classDef success fill:#d4edda,stroke:#28a745,color:#155724;\n';
    code += 'classDef running fill:#fff3cd,stroke:#ffc107,color:#856404;\n';
    code += 'classDef failed fill:#f8d7da,stroke:#dc3545,color:#721c24;\n';
    code += 'classDef pending fill:#e2e3e5,stroke:#6c757d,color:#383d41;\n';

    return code;
}

// 用于处理不同种类的task结果信息的显示
export function renderHtmlContent(htmlContent, result){
    switch (result.type) {
        case 'text':
            htmlContent += `<pre>${result.content}</pre>`;
            break;
        case 'dataframe':
            // 将JSON字符串解析为对象，并渲染为表格
            let data;
            try {
                data = JSON.parse(result.content);
                htmlContent += `<h4>DataFrame</h4>` + createTableFromJSON(data);
            } catch (e) {
                htmlContent += `<p>数据解析失败: ${e.message}</p>`;
            }
            break;
        case 'image':
            // 使用 Base64 编码的图片数据渲染<img>标签，假设图片类型为jpeg，可以根据实际情况修改
            htmlContent += `<h4>图片</h4><img src="data:image/jpeg;base64,${result.content}" alt="Task Result Image" style="max-width:100%;" />`;
            break;
        case 'json':
            let json_data;
            try {
                json_data = JSON.parse(result.content);
                htmlContent += `<h4>Json Content</h4>` + json_data.data;
            } catch (e) {
                htmlContent += `<p>数据解析失败: ${e.message}</p>`;
            }
            break;
        case 'error':
            // 显示后端返回的错误信息
            htmlContent += `<p class="status-badge status-failed">处理结果失败: ${result.content}</p>`;
            break;
        default:
            htmlContent += `<p>未知结果类型: ${result.type}</p>`;
            break;
    }
    return htmlContent;
}

// 用于渲染显示task结果的df表格
function createTableFromJSON(data) {
    if (!data || !data.columns || !data.data) {
        return '<div>无效的 DataFrame 数据</div>';
    }
    let tableHtml = '<table><thead><tr>';
    data.columns.forEach(col => {
        tableHtml += `<th>${col}</th>`;
    });
    tableHtml += '</tr></thead><tbody>';
    data.data.forEach(row => {
        tableHtml += '<tr>';
        row.forEach(cell => {
            tableHtml += `<td>${cell}</td>`;
        });
        tableHtml += '</tr>';
    });
    tableHtml += '</tbody></table>';
    return tableHtml;
}