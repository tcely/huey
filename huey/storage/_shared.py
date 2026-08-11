import time

from huey.constants import EmptyData
from huey.exceptions import ConfigurationError

def convert_ts(ts):
    return time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)

def int_time(multiplier):
    return int(time.time() * multiplier)
