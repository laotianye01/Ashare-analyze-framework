from core.resource_manager import ResourceManager
from utils.database_utils import *
from utils.dataframe_utils import *
import pandas as pd
import matplotlib.pyplot as plt
import mplfinance as mpf
from core.taskNode import TaskNode


class DrawGraph(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            # 获取数据
            df = params.get("data").get("data")
            
            # 检查数据是否为空（存在公司存在但无历史数据的情况）
            if df is None or df.empty:
                return {"status": "success", "data": None, "error": "No data available to plot."}
            
            df['update_time'] = pd.to_datetime(df['update_time'])
            df = df.sort_values(by='update_time')
            df.set_index('update_time', inplace=True)

            # 只保留绘图需要的列，并重命名以符合 mplfinance 要求
            ohlc = df[['open', 'high', 'low', 'close']].copy()
            ohlc.columns = ['Open', 'High', 'Low', 'Close']  # 必须使用这些列名

            # 绘制蜡烛图
            mpf.plot(
                ohlc,
                type='candle',
                style='charles',
                title='K-line Chart (Candlestick)',
                ylabel='Price',
                figsize=(10, 6),
                savefig='./report/kline_chart.png'
            )

            return {"status": "success", "data": "", "error": None}

        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}