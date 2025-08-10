// render.js
// TODO: 当项目较大，且有较多DOM时，事件的处理可以通过事件委托，即通过顶层DOM监听子DOM触发的事件，并执行对应的处理

import {
    schedulerList, pipelineView, pipelineNameSpan,
    pipelineStatusDiv, taskDetailsPanel, taskNameSpan, taskInfoDiv,
    noSelectionPanel, pipelineConfigSelect, startBtn, configEditor
} from './script.js';

import { buildMermaidCode, renderHtmlContent } from './util.js'
import {
    handleStopScheduler,
    handleTogglePipelineList, 
    handleMermaidNodeClick, 
    bindTaskClickEvents,
    handleSelectPipelineAndScheduler,
    handleSaveResultRequest,
} from './eventHandlers.js';

// 初始化 Mermaid.js 配置 --- Mermaid为用于渲染pipeline流程图的组建
mermaid.initialize({
    theme: 'default'
});

// 徐娜然配置文件列表
export function renderConfigs(configFilesMeta) {
    pipelineConfigSelect.innerHTML = '';
    
    if (configFilesMeta.length === 0) {
        const option = document.createElement('option');
        option.value = '';
        option.textContent = '未找到配置文件';
        option.disabled = true;
        pipelineConfigSelect.appendChild(option);
        startBtn.disabled = true;
    } else {
        startBtn.disabled = false;
        configFilesMeta.forEach(config => {
            const option = document.createElement('option');
            option.value = config.path;
            option.textContent = config.name;
            pipelineConfigSelect.appendChild(option);
        });
    }
}

// 用于渲染配置的json信息
export function renderConfigEditorContent(conf_json) {
    configEditor.value = conf_json;
}

// 渲染调度器列表
export function renderSchedulerList(currentData, selectedSchedulerId, expandedSchedulers, selectedPipelineName) {
    schedulerList.innerHTML = '';
    console.info("currentData: ", currentData);
    for (const schId in currentData) {
        const schedulerItem = document.createElement('li');
        schedulerItem.className = `scheduler-item ${schId === selectedSchedulerId ? 'active' : ''}`;
        schedulerItem.setAttribute('data-scheduler-id', schId);

        const schedulerHeader = document.createElement('div');
        schedulerHeader.className = 'scheduler-header';

        const schedulerIdSpan = document.createElement('span');
        schedulerIdSpan.textContent = `Scheduler ID: ${schId}`;
        schedulerIdSpan.onclick = () => {
            // 点击时，切换展开状态
            handleTogglePipelineList(schId, currentData);
        };
        
        const stopBtn = document.createElement('button');
        stopBtn.textContent = '终止';
        stopBtn.className = 'stop-btn';
        stopBtn.onclick = (event) => {
            event.stopPropagation();
            if (confirm(`确定要终止调度器 ${schId} 吗？`)) {
                handleStopScheduler(schId);
            }
        };

        schedulerHeader.appendChild(schedulerIdSpan);
        schedulerHeader.appendChild(stopBtn);
        schedulerItem.appendChild(schedulerHeader);

        const pipelineList = document.createElement('ul');
        pipelineList.className = 'pipeline-list';
        // 关键修改：检查 Set 中是否有该调度器ID，以决定是否展开
        if (expandedSchedulers.has(schId)) {
            pipelineList.classList.add('visible');
        }

        const schedulerData = currentData[schId];
        console.info("schedulerData: ", schedulerData);
        if (schedulerData) {
            console.info("pipelineName in schedulerData");
            for (const pipelineName in schedulerData) {
                const pipelineItem = document.createElement('li');
                pipelineItem.textContent = pipelineName;
                pipelineItem.className = `pipeline-item ${pipelineName === selectedPipelineName && schId === selectedSchedulerId ? 'active' : ''}`;
                pipelineItem.onclick = () => {
                    handleSelectPipelineAndScheduler(schId, pipelineName, currentData);
                };
                console.info("pipelineItem.onclick added!");
                pipelineList.appendChild(pipelineItem);
            }
        }

        schedulerItem.appendChild(pipelineList);
        schedulerList.appendChild(schedulerItem);
    }
}

// 用于渲染选中的pipeline mermaid流程图
export function renderPipeline(pipelineName, pipelineData) {
    pipelineView.classList.remove('hidden');
    pipelineNameSpan.textContent = pipelineName;
    pipelineStatusDiv.textContent = pipelineData.status;
    pipelineStatusDiv.className = `status-badge status-${pipelineData.status}`;
    const mermaidContainer = document.querySelector('.mermaid');
    
    if (!pipelineData.tasks || Object.keys(pipelineData.tasks).length === 0) {
        mermaidContainer.innerHTML = '<div>此流水线没有任务可显示。</div>';
        taskDetailsPanel.classList.add('hidden');
        return;
    }
    
    const mermaidCode = buildMermaidCode(pipelineData);
    mermaid.render('graphDiv', mermaidCode)
        .then(({ svg, bindFunctions }) => {
            mermaidContainer.innerHTML = svg;
            bindTaskClickEvents(pipelineData);
        })
        .catch(error => {
            console.error('Mermaid 渲染失败:', error);
            mermaidContainer.innerHTML = '<div>渲染图表失败，请检查控制台。</div>';
        });
    console.log('pipline selected:', pipelineName);
}

// 渲染（绑定） Mermaid 图中task节点的点击事件 (使用事件委托)
export function renderMermaidContainer(pipelineData){
    const mermaidContainer = document.querySelector('.mermaid');
    mermaidContainer.addEventListener('click', (event) => {
        handleMermaidNodeClick(event, pipelineData);
    });
}

// 渲染任务详情面板
export function renderTaskDetails(task, currentTaskResult) {
    taskDetailsPanel.classList.remove('hidden');
    taskNameSpan.textContent = task.name_id;
    
    let htmlContent = `
        <p><strong>状态:</strong> <span class="status-badge status-${task.status}">${task.status}</span></p>
        <p><strong>重试次数:</strong> ${task.retry_times}</p>
        <h4>任务结果:</h4>
    `;

    // 检查 task.result 是否存在且结构正确
    const result = task.result;
    if (result && result.type && result.content !== undefined) {
        htmlContent = renderHtmlContent(htmlContent, result);
        // 添加保存按钮
        htmlContent += `<button id="save-result-btn" class="save-btn">保存结果</button>`;
    } else {
        htmlContent += `<p>无结果</p>`;
    }

    taskInfoDiv.innerHTML = htmlContent;

    // 为保存按钮绑定事件监听器（使用匿名函数传递参数）
    const saveBtn = document.getElementById('save-result-btn');
    if (saveBtn) {
        saveBtn.addEventListener('click', () => handleSaveResultRequest(currentTaskResult));
    }
}

// ----------------------------------- 以下函数用于清空配置 --------------------------------- //
export function renderHidePipelineTask(){
    pipelineView.classList.add('hidden');
    taskDetailsPanel.classList.add('hidden');
}

export function renderResetRightPanel(){
    noSelectionPanel.classList.remove('hidden');
    pipelineView.classList.add('hidden');
    taskDetailsPanel.classList.add('hidden');
}
