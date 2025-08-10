import akshare as ak
from core.taskNode import TaskNode
from core.resource_manager import ResourceManager


"""
This module provides functions to fetch various types of Chinese market data, 
It relies on the AkShare library to interact with financial data sources.

This module encapsulates akshare package, which standardizes all functions.
"""


# fetch latest Chinese reserve ratio
class FetchReserveRatio(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.macro_china_reserve_requirement_ratio()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch latest Chinese interest rates
class FetchIntrestRate(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.macro_china_lpr()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}

# fetch latest Chinese mid price
class FetchCurrentMidPrice(TaskNode):
    def _custom_task(self, resource_config, params=None):
        try:
            data = ak.currency_boc_safe()
            return {"status": "success", "data": data, "error": None}
        except Exception as e:
            return {"status": "failed", "data": None, "error": str(e)}
