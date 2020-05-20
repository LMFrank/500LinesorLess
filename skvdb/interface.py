# -*- coding: utf-8 -*-
from physical import Storage
from binary_tree import BinaryTree


class DB(object):
    """将二叉树的操作封装成dict键值对操作"""

    def __init__(self, f):
        self._storage = Storage(f)
        self._tree = BinaryTree(self._storage)

    def _assert_not_closed(self):
        """检查文件是否关闭"""
        if self._storage.closed:
            raise ValueError('Database closed.')

    def close(self):
        self._storage.close()

    def commit(self):
        """提交后才会更新数据库"""
        self._assert_not_closed()
        self._tree.commit()

    def __getitem__(self, key):
        """通过 db[key] 获取值"""
        pass

    def __setitem__(self, key, value):
        """通过 db[key] = value 设置值"""
        pass

    def __delitem__(self, key):
        """通过 del db[key] 删除值"""
        pass

    def __contains__(self, key):
        """通过 key in db 判断键是否在数据库"""
        try:
            self[key]
        except KeyError:
            return False
        else:
            return True

    def __len__(self):
        return len(self._tree)