import numpy as np

class HashSet:
    def __init__(self, values=None, prime=97, default=-9999):
        self.hashset = {}
        for i in xrange(prime):
            self.hashset[i] = {}
        self.row_dim = 1
        if values is not None:
            for i in xrange(values.shape[0]):
                uid = int(values[i, 0])
                gid = uid % 97
                if values.shape[1] == 2:
                    self.row_dim = 1
                    self.hashset[gid][uid] = values[i, 1]
                else:
                    self.row_dim = values.shape[1] - 1
                    self.hashset[gid][uid] = list(values[i, 1:])
        self.default = default
    def get(self, key_id, default=None):
        key_id = int(key_id)
        gid = key_id % 97
        if self.row_dim == 1:
            if key_id not in self.hashset[gid]:
                self.hashset[gid][key_id] = default
                return default
            else:
                return self.hashset[gid][key_id]
        else:
            if key_id not in self.hashset[gid]:
                self.hashset[gid][key_id] = default
            else:
                return self.hashset[gid][key_id]
    def add_one(self, key_id):
        key_id = int(key_id)
        gid = key_id % 97
        if key_id not in self.hashset[gid]:
            self.hashset[gid][key_id] = 0.0
        self.hashset[gid][key_id] += 1
    def merge(self, other, dft=-9999):
        for gid in self.hashset:
            for key_id in self.hashset[gid]:
                if other.has(key_id):
                    other_value = other.get(key_id)
                else:
                    other_value = dft
                if self.row_dim == 1:
                    self.hashset[gid][key_id] = [self.hashset[gid][key_id], other_value]
                else:
                    self.hashset[gid][key_id].append(other_value)
                assert len(self.hashset[gid][key_id]) == self.row_dim + 1
        self.row_dim += 1
        return self

    def merge_op(self, other, op, dft=-9999):
        new_hashset = HashSet()
        for gid in self.hashset:
            for key_id in self.hashset[gid]:
                if not self.has(key_id) or not other.has(key_id):
                    new_hashset.hashset[gid][key_id] = dft
                else:
                    new_hashset.hashset[gid][key_id] = op(self.hashset[gid][key_id], other.get(key_id))
        return new_hashset
    def has(self, key_id):
        key_id = int(key_id)
        gid = key_id % 97
        return key_id in self.hashset[gid]
    def set(self, key_id, value):
        key_id = int(key_id)
        gid = key_id % 97
        self.hashset[gid][key_id] = value
    def get_keys(self):
        key_set = set()
        for uid in self.hashset:
            for mid in self.hashset[uid]:
                key_set.add(mid)
        return key_set


"""
import numpy as np

class HashSet:
    def __init__(self, values=None, prime=97, default=-9999):
        self.hashset = {}
        for i in xrange(prime):
            self.hashset[i] = {}
        self.row_dim = 1
        if values is not None:
            for i in xrange(values.shape[0]):
                uid = int(values[i, 0])
                gid = uid % 97
                if values.shape[1] == 2:
                    self.row_dim = 1
                    self.hashset[gid][uid] = values[i, 1]
                else:
                    self.row_dim = values.shape[1] - 1
                    self.hashset[gid][uid] = list(values[i, 1:])
        self.default = default
    def get(self, key_id):
        key_id = int(key_id)
        gid = key_id % 97
        if self.row_dim == 1:
            return self.hashset[gid].get(key_id, self.default)
        else:
            return self.hashset[gid].get(key_id, [self.default for i in xrange(self.row_dim)])
    def add_one(self, key_id):
        key_id = int(key_id)
        gid = key_id % 97
        if key_id not in self.hashset[gid]:
            self.hashset[gid][key_id] = 0.0
        self.hashset[gid][key_id] += 1
    def merge(self, other, dft=-9999):
        for gid in self.hashset:
            for key_id in self.hashset[gid]:
                if other.has(key_id):
                    other_value = other.get(key_id)
                else:
                    other_value = dft
                if self.row_dim == 1:
                    self.hashset[gid][key_id] = [self.hashset[gid][key_id], other_value]
                else:
                    self.hashset[gid][key_id].append(other_value)
                assert len(self.hashset[gid][key_id]) == self.row_dim + 1
        self.row_dim += 1
        return self

    def merge_op(self, other, op, dft=-9999):
        new_hashset = HashSet()
        for gid in self.hashset:
            for key_id in self.hashset[gid]:
                if not self.has(key_id) or not other.has(key_id):
                    new_hashset.hashset[gid][key_id] = dft
                else:
                    new_hashset.hashset[gid][key_id] = op(self.hashset[gid][key_id], other.get(key_id))
        return new_hashset
    def has(self, key_id):
        key_id = int(key_id)
        gid = key_id % 97
        return key_id in self.hashset[gid]
    def set(self, key_id, value):
        key_id = int(key_id)
        gid = key_id % 97
        self.hashset[gid][key_id] = value
"""
