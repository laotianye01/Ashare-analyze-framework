import { SCHEDULERS_API_URL, CONFIGS_API_URL } from './constants.js';

export async function fetchSchedulersApi() {
    try {
        const response = await fetch(SCHEDULERS_API_URL);
        if (!response.ok) {
            // 抛出带有详细信息的错误
            throw new Error(`网络请求失败: ${response.statusText}`);
        }
        return await response.json();
    } catch (error) {
        // 在这里进行日志记录
        console.error('获取调度器数据失败:', error);
        // 重新抛出错误，让调用者决定如何处理（例如，显示给用户）
        throw error;
    }
}

export async function fetchConfigsApi() {
    try {
        const response = await fetch(CONFIGS_API_URL);
        if (!response.ok) {
            throw new Error('获取配置文件列表失败');
        }
        return await response.json();
    } catch (error) {
        console.error('获取配置文件列表时发生错误:', error);
        throw error;
    }
}

export async function fetchConfigContentApi(configName) {
    try {
        const parts = configName.split('/');
        const fileName = parts[parts.length - 1];
        const fileNameWithoutExtension = fileName.replace(/\.json$/, '');
        const url = `${CONFIGS_API_URL}/${fileNameWithoutExtension}`;
        
        const response = await fetch(url);
        
        if (!response.ok) {
            throw new Error(`获取配置文件 ${fileName} 内容失败`);
        }
        
        return await response.json();
    } catch (error) {
        console.error('获取配置文件内容时发生错误:', error);
        throw error;
    }
}

export async function createSchedulerApi(pipelinePath, updatesData = null) {
    const requestBody = {
        pipeline_path: pipelinePath
    };
    if (updatesData) {
        requestBody.updates = updatesData.updates;
    }

    const response = await fetch('/schedulers/create', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json'
        },
        body: JSON.stringify(requestBody)
    });

    if (!response.ok) {
        const errorResult = await response.json();
        throw new Error(`启动调度器失败: ${errorResult.error || '未知错误'}`);
    }

    return await response.json();
}

export async function stopSchedulerApi(schedulerId) {
    const response = await fetch(`${SCHEDULERS_API_URL}/${schedulerId}/stop`, {
        method: 'POST'
    });

    if (!response.ok) {
        throw new Error('终止调度器失败');
    }

    return await response.json();
}