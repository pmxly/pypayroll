#!/usr/bin/env python3
# -*- coding: utf-8 -*-


import os
import platform


def get_prj_root():
    """
    :return: project root path
    """
    return os.path.dirname(os.path.abspath(__file__))


def get_prj_path():
    """
    :return: project parent directory
    """
    return os.path.dirname(get_prj_root()) + get_sep()


def get_task_pool():
    """
    :return: task definition root path
    """
    return os.path.join(get_prj_path(), 'hhr', 'taskspool') + get_sep()


def get_task_output_path():
    """
    :return: task file output path
    """
    return os.path.join(get_prj_root(), 'output', 'task') + get_sep()


def get_sep():
    """
    :return: file path separator for current os system
    """
    if 'Windows' in platform.system():
        sep = '\\'
    else:
        sep = '/'
    return sep
