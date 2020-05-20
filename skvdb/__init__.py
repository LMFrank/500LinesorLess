# -*- coding: utf-8 -*-
import os

from interface import DB


__all__ = ['DB', 'connect']


def connect(dbname):
    """
    import skvdb

    conn = skvdb.connect(dbname)

    :param dbname: 数据库名称
    :return:
    """
    try:
        f = open(dbname, 'r+b')  # 可读可追加，但不可以覆盖已有数据
    # 如果文件不能在则建立一个新的数据库文件
    except IOError:
        fd = os.open(dbname, os.O_RDWR | os.O_CREAT)
        f = os.fdopen(fd, 'r+b')
    return DB(f)