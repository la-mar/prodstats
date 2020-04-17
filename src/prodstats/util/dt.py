from datetime import datetime

import pytz

from util.iterables import AttrDict
from util.jsontools import make_repr

__FORMATS__ = {"no_seconds": "%Y-%m-%d %H:%m"}


class DateTimeFormats(AttrDict):
    def __repr__(self):
        return make_repr(self.__dict__)


def utcnow():
    """ Get the current datetime in utc as a datetime object with timezone information """
    return datetime.now().astimezone(pytz.utc)


formats = DateTimeFormats(__FORMATS__)
