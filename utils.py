__author__ = 'Zakaria'

import parameters
import time


def set_subwindow_time(t):
    """
    Sets timeout for processors. time based subwindow
    :param time:
    """
    parameters.subwindow_time = t


def get_millisecond():
    """
    Returns current time in millisecond
    :return: current time in ms
    """
    return int(round(time.time() * 1000))


def get_second():
    """
    Returns current time in second
    :return: current time in second
    """
    return int(round(time.time()))