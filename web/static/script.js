// script.js

import { REFRESH_INTERVAL } from './constants.js';
import { 
    handleStartButtonClick, 
    handleFetchAndRenderConfigs, 
    handleFetchAndRenderSchedulers,
    handleConfigSelectChange
} from './eventHandlers.js';

// === 将所有DOM元素的引用声明为全局变量 ===
export let schedulerList, pipelineView, pipelineNameSpan, pipelineStatusDiv,
    taskDetailsPanel, taskNameSpan, taskInfoDiv, noSelectionPanel,
    refreshBtn, pipelineConfigSelect, startBtn, configEditor;

// 初始加载和定时刷新
document.addEventListener('DOMContentLoaded', () => {
    // 通过元素的id找到并加载index中(可能)会被js修改的元素的引用，对其进行修改即可改变对应html上的显示
    // ex: 通过id=scheduler-list找到html中对应元素的位置，并将其赋给schedulerList
    schedulerList = document.getElementById('scheduler-list');
    pipelineView = document.getElementById('pipeline-view');
    pipelineNameSpan = document.getElementById('pipeline-name');
    pipelineStatusDiv = document.getElementById('pipeline-status');
    taskDetailsPanel = document.getElementById('task-details');
    taskNameSpan = document.getElementById('task-name');
    taskInfoDiv = document.getElementById('task-info');
    noSelectionPanel = document.getElementById('no-selection');
    refreshBtn = document.getElementById('refresh-btn');
    pipelineConfigSelect = document.getElementById('pipeline-config');
    startBtn = document.getElementById('start-btn');
    configEditor = document.getElementById('config-editor');

    handleFetchAndRenderConfigs();
    handleFetchAndRenderSchedulers();
    setInterval(handleFetchAndRenderSchedulers, REFRESH_INTERVAL);

    refreshBtn.addEventListener('click', handleFetchAndRenderSchedulers);

    startBtn.addEventListener('click', () => {
        handleStartButtonClick(pipelineConfigSelect, configEditor);
    });

    // 为配置文件下拉菜单添加事件监听器
    pipelineConfigSelect.addEventListener('change', handleConfigSelectChange);
});