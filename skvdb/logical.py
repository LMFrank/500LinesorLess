# -*- coding: utf-8 -*-
class ValueRef(object):
    """对数据库中数据的引用"""

    def __init__(self, referent=None, address=0):
        self._referent = referent  # 引用的值
        self._address = address  # 值的位置

    def prepare_to_store(self, storage):
        # 存储引用对象前的其它操作，自定义
        pass

    @staticmethod
    def referent_to_string(referent):
        return referent.encode('utf-8')

    @staticmethod
    def string_to_referent(string):
        return string.decode('utf-8')

    @property
    def address(self):
        return self._address

    def get(self, storage):
        if self._referent is None and self._address:
            # 将从文件中读取的字节串转换为Python中引用的对象
            self._referent = self.string_to_referent(storage.read(self._address))
        return self._referent

    def store(self, storage):
        # 引用对象不为空而地址为空说明该引用对象还未被存储过
        if self._referent is not None and not self._address:
            self.prepare_to_store(storage)
            # 得到引用对象在文件中的地址
            self._address = storage.write(self.referent_to_string(self._referent))


class LogicalBase(object):
    node_ref_class = None  # 对数据结构节点的引用，会在子类中赋值
    value_ref_class = ValueRef  # 对值的引用

    def __init__(self, storage):
        self._storage = storage
        self._refresh_tree_ref()

    def commit(self):
        """提交数据"""
        self._tree_ref.store(self._storage)  # 存储引用的树
        self._storage.commit_root_address(self._tree_ref.address)  # 更新树的根节点的地址

    def _refresh_tree_ref(self):
        self._tree_ref = self.node_ref_class(address=self._storage.get_root_address())

    def get(self, key):
        """获取键值"""
        # 如果数据库文件没有上锁，则更新对树的引用
        if not self._storage.locked:
            self._refresh_tree_ref()
        return self._get(self._follow(self._tree_ref), key)

    def set(self, key, value):
        """更新键值"""
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._insert(self._follow(self._tree_ref), key, self.value_ref_class(value))

    def pop(self, key):
        """删除键值"""
        if self._storage.lock():
            self._refresh_tree_ref()
        self._tree_ref = self._delete(self._follow(self._tree_ref), key)

    def _follow(self, ref):
        """获取Ref所引用的具体对象。"""
        return ref.get(self._storage)

    def __len__(self):
        if not self._storage.locked:
            self._refresh_tree_ref()
        root = self._follow(self._tree_ref)  # 获取二叉树的根节点
        if root:
            return root.length
        else:
            return 0