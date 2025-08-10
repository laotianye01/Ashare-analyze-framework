// eventHandler.js
// 导入渲染所使用的函数
import { 
    renderConfigs, 
    renderConfigEditorContent, 
    renderPipeline, 
    renderTaskDetails, 
    renderSchedulerList,
    renderHidePipelineTask,
    renderMermaidContainer,
    renderResetRightPanel
} from './render.js'
import {    
    fetchSchedulersApi, 
    fetchConfigsApi, 
    fetchConfigContentApi, 
    createSchedulerApi, 
    stopSchedulerApi,
} from './apiService.js'
import {
    pipelineConfigSelect, configEditor
} from './script.js';

// 以下定义了web的一些运行状态
let selectedSchedulerId = null;
let selectedPipelineName = null;
let selectedTaskId = null;
// 用于存储哪些调度器的流水线列表是展开的
let expandedSchedulers = new Set();
// 用于存储当前任务结果
let currentTaskResult = null;
// 用于存储当前选中的配置文件内容
let currentConfigContent = null;

// 处理列表渲染
export async function handleFetchAndRenderConfigs(configName=null) {
    try {
        // 在渲染完列表后，主动加载第一个配置文件的内容
        if (configName != null) {
            // 调用 apiService.js 中的函数获取数据
            const configContent = await fetchConfigContentApi(configName);
            currentConfigContent = configContent;
            renderConfigEditorContent(JSON.stringify(configContent, null, 2));
        } else{
            const configFilesMeta = await fetchConfigsApi();
            renderConfigs(configFilesMeta); // 仅渲染下拉列表
        }

    } catch (error) {
        renderConfigs([]);
        console.error('渲染配置文件列表失败:', error);
    }
}

// 处理选择事件
export function handleConfigSelectChange(event) {
    const selectedConfigName = event.target.value;
    if (selectedConfigName) {
        handleFetchAndRenderConfigs(selectedConfigName);
    }
}

// 用与启动新的调度器，与渲染面板
export async function handleStartButtonClick(pipelineConfigSelect, configEditor) {
    const pipelinePath = pipelineConfigSelect.value;
    if (!pipelinePath) {
        alert('请选择一个流水线配置。');
        return;
    }
    try {
        const modifiedConfigString = configEditor.value;
        let updatesData = null;
        // 仅当编辑器中的内容与原始加载的内容不同时，才发送 updates
        if (modifiedConfigString !== JSON.stringify(currentConfigContent, null, 2)) {
            const modifiedConfig = JSON.parse(modifiedConfigString);
            updatesData = {
                updates: modifiedConfig
            };
        }
        // 调用新函数，同时传递路径和可选的更新数据
        try {
            // 调用 apiService 中的函数来启动调度器
            const result = await createSchedulerApi(pipelinePath, updatesData);
            console.log(result.message);
            await handleFetchAndRenderSchedulers();

        } catch (error) {
            console.error('启动调度器时发生错误:', error);
            alert(error.message);
        }
    } catch (error) {
        console.error('解析配置时发生错误:', error);
        alert('配置格式不正确，请检查 JSON 语法。');
    }
}

// 终止调度器函数
export async function handleStopScheduler(schedulerId) {
    try {
        // 调用 apiService 中的函数来终止调度器
        const result = await stopSchedulerApi(schedulerId);
        console.log(result.message);
        // 终止成功后，立即刷新状态列表
        await handleFetchAndRenderSchedulers();
        // 如果终止的是当前选中的调度器，则清空右侧面板
        if (selectedSchedulerId === schedulerId) {
            // 这里我们不假设有 resetUI 函数，而是直接执行UI重置逻辑
            selectedSchedulerId = null;
            selectedPipelineName = null;
            renderResetRightPanel();
        }

    } catch (error) {
        console.error('终止调度器时发生错误:', error);
        alert('终止调度器失败，请检查控制台。');
    }
}

// 获取所有调度器状态并渲染列表（也用于定期更新）
export async function handleFetchAndRenderSchedulers() {
    try {
        // 调用 apiService 中的函数获取数据
        const schedulerData = await fetchSchedulersApi();
        renderSchedulerList(schedulerData, selectedSchedulerId, expandedSchedulers, selectedPipelineName);
        if (selectedSchedulerId && selectedPipelineName){
            const selectSchedulerData = schedulerData[selectedSchedulerId];
            console.info("schedulerData: ", selectSchedulerData);
            if (selectSchedulerData && selectSchedulerData[selectedPipelineName]){
                const pipelineData = selectSchedulerData[selectedPipelineName];
                handleSelectPipeline(schedulerData, selectedPipelineName);
                if (selectedTaskId){
                    const taskDetails = pipelineData.tasks[selectedTaskId];
                    if (taskDetails) {
                        renderTaskDetails(taskDetails, currentTaskResult);
                        handleRenderTaskDetails(taskDetails);
                    }
                }
            } else{
                selectedSchedulerId = null;
                selectedPipelineName = null;
                renderResetRightPanel();
            }
        }
    } catch (error) {
        console.error('获取调度器数据失败:', error);
    }
}

// 切换流水线列表的可见性
export function handleTogglePipelineList(schId, currentData) {
    // 关键修改：使用 Set 来记录展开状态
    if (expandedSchedulers.has(schId)) {
        expandedSchedulers.delete(schId);
    } else {
        expandedSchedulers.add(schId);
    }
    // 重新渲染列表以应用新的展开状态
    renderSchedulerList(currentData, selectedSchedulerId, expandedSchedulers, selectedPipelineName);
}

// 选中调度器和流水线，并更新右侧面板
export function handleSelectPipelineAndScheduler(schId, pipelineName, currentData) {
    selectedSchedulerId = schId;
    selectedPipelineName = pipelineName;

    // 重新渲染左侧列表以更新激活状态
    renderSchedulerList(currentData, selectedSchedulerId, expandedSchedulers, selectedPipelineName); 
    // 调用现有的函数来渲染右侧面板
    handleSelectPipeline(currentData, pipelineName);
}

// 用于处理pipeline相关函数
function handleSelectPipeline(currentData, pipelineName){
    const pipelineData = currentData[selectedSchedulerId] ? currentData[selectedSchedulerId][pipelineName] : null;
    console.info("pipelineData:", pipelineData);
    if (!pipelineData){
        renderHidePipelineTask();
        return;
    }
    console.info("rander pipeline!");
    renderPipeline(pipelineName, pipelineData);
}

// 用于处理Mermaid流程图的点击事件
export function handleMermaidNodeClick(event, pipelineData) {
    const clickedNode = event.target.closest('.node');

    if (clickedNode) {
        const taskId = clickedNode.id;
        // 使用更精确的正则表达式来匹配原始任务ID
        const originalTaskIdMatch = taskId.match(/flowchart-([a-zA-Z0-9_-]+)-\d+/);

        if (originalTaskIdMatch) {
            const originalTaskId = originalTaskIdMatch[1];
            
            if (pipelineData && pipelineData.tasks && pipelineData.tasks[originalTaskId]) {
                const taskDetails = pipelineData.tasks[originalTaskId];
                selectedTaskId = originalTaskId; 
                renderTaskDetails(taskDetails, currentTaskResult);
                handleRenderTaskDetails(taskDetails);
            } else {
                console.warn('任务ID未在数据中找到:', originalTaskId);
            }
        } else {
            console.warn('节点ID格式不匹配:', taskId);
        }
    }
}

// 用于为currentTaskResult赋值，其作用为浜村对应信息
function handleRenderTaskDetails(task){
    // 检查 task.result 是否存在且结构正确
    const result = task.result;
    if (result && result.type && result.content !== undefined) {
        currentTaskResult = task.result;
    }
}

export function bindTaskClickEvents(pipelineData) {
    const mermaidContainer = document.querySelector('.mermaid');
    if (!mermaidContainer) {
        console.error('未找到 Mermaid 容器');
        return;
    }
    renderMermaidContainer(pipelineData);
}

// 用于处理点击保存时的请求
export function handleSaveResultRequest(currentTaskResult) {
    if (!currentTaskResult) {
        alert('没有可保存的任务结果。');
        return;
    }

    const result = currentTaskResult;
    let content = result.content; // 使用let以便可以修改
    const type = result.type;
    let fileName = `task_result_${new Date().toISOString().slice(0, 10)}`;
    let mimeType = '';

    switch (type) {
        case 'text':
            fileName += '.txt';
            mimeType = 'text/plain';
            break;
            
        case 'dataframe':
            fileName += '.csv';
            mimeType = 'text/csv';
            try {
                // 解析后端返回的 orient='split' 格式的JSON
                const data = JSON.parse(content);
                const columns = data.columns;
                const rows = data.data;

                // 构建CSV内容
                let csvContent = columns.join(',') + '\n'; // CSV头部
                rows.forEach(row => {
                    csvContent += row.join(',') + '\n'; // CSV行数据
                });

                const bom = "\ufeff";
                content = [bom + csvContent]; // 创建一个数组，包含BOM和CSV内容
            } catch (e) {
                alert('解析DataFrame数据失败，无法保存为CSV。');
                console.error('解析JSON失败:', e);
                return;
            }
            break;
            
        case 'json':
            fileName += '.json';
            mimeType = 'application/json';
            try {
                const data = JSON.parse(content);
                content = JSON.stringify(data, null, 2);
            } catch (e) {
                console.error('格式化JSON失败:', e);
            }
            break;

        case 'image':
            fileName += '.jpeg';
            mimeType = 'image/jpeg';
            downloadDataUrl(`data:image/jpeg;base64,${content}`, fileName);
            return;
            
        case 'error':
            fileName += '.log';
            mimeType = 'text/plain';
            break;
            
        default:
            fileName += '.txt';
            mimeType = 'text/plain';
            break;
    }
    // 创建 Blob 对象
    const blob = new Blob(content, { type: mimeType + ';charset=utf-8;' }); // 明确指定字符集
    const url = URL.createObjectURL(blob);
    
    // 模拟点击下载链接
    const a = document.createElement('a');
    a.href = url;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    
    // 释放 URL 对象
    URL.revokeObjectURL(url);
}

// 辅助函数：用于下载数据URL（主要用于图片）
function downloadDataUrl(dataUrl, fileName) {
    const a = document.createElement('a');
    a.href = dataUrl;
    a.download = fileName;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
}