import os
import time
import datetime
import uuid
import math

from elastic.base import Node


def get_timestamp():
    now = datetime.datetime.now()
    time_string = now.strftime("%Y%m%d%H%M%S")
    return time_string


def get_tmp_file():
    return "/tmp/%s" % str(uuid.uuid4())


def parse_node(s):
    result = s.split('=')
    return Node(result[0], result[1])


def find_nearest_power_2(n):
    return pow(2, int(math.log(n, 2) + 0.5))


def string_grep(s, word):
    for line in s.split("\n"):
        if word in line:
            return line
    return None