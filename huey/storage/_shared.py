import hashlib
import re
import time

from huey.constants import EmptyData
from huey.exceptions import ConfigurationError


def clean_name(name):
    return re.sub('[^A-Za-z0-9_]', '', name)

def convert_ts(ts):
    return time.mktime(ts.timetuple()) + (ts.microsecond * 1e-6)

def hexdigest(data_bytes):
    if isinstance(data_bytes, str):
        data_bytes = data_bytes.encode('utf8')
    return hashlib.md5(data_bytes).hexdigest()

def int_time(multiplier):
    return int(time.time() * multiplier)
