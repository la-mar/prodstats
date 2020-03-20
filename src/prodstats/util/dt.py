from datetime import datetime

import pytz


def utcnow():
    """ Get the current datetime in utc as a datetime object with timezone information """
    return datetime.now().astimezone(pytz.utc)
