__author__ = 'Zakaria'

import parameters
import time


def set_subwindow_time(time):
    """
    Sets timeout for processors. time based subwindow
    :param time:
    """
    parameters.parameter_sw_time = time


def get_millisecond():
    return int(round(time.time() * 1000))


def get_second():
    return int(round(time.time()))