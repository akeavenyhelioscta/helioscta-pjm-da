from datetime import datetime
from zoneinfo import ZoneInfo

"""
"""

def get_mst_timestamp():
    mst = ZoneInfo("America/Denver")
    mst_timestamp = datetime.now(mst)
    return mst_timestamp