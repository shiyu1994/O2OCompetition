from feature_index import FeatureIndex
from hashset import HashSet
import numpy as np
from code.metainfo import paths

class Matrix:
    def __init__(self, matrix, col_names, col_formats=["%s"], default=0.0):
        self.col_index = FeatureIndex()
        self.matrix = matrix
        self.col_names = col_names
        self.ndata = matrix.shape[0]
        for name in col_names:
            self.col_index.register(name)
        self.col_formats = col_formats
        self.default = default
    def check_point(self, matrix_name):
        np.savetxt(paths.my_path + '{0}.csv'.format(matrix_name), self.matrix, fmt=",".join(self.col_formats))
        col_names_file = open(paths.my_path + '{0}_col_names.csv'.format(matrix_name), 'w')
        col_names_file.write(",".join(self.col_names) + "\n")
        col_names_file.write(",".join(str(self.col_index.get_index(name)) for name in self.col_names) + "\n")
        col_names_file.close()

    def join(self, key_name, feature_names, values, formats, dft=0.0):
        """
        if len(feature_names) == 1:
            self.col_index.register(feature_names[0])
            self.col_names.append(feature_names)
            self.matrix = np.hstack((self.matrix, np.zeros((self.matrix.shape[0], 1))))
            self.col_formats.append(formats)
        else:
        """
        for name in feature_names:
            self.col_index.register(name)
            self.col_names.append(name)
        for fmt in formats:
            self.col_formats.append(fmt)
        self.matrix = np.hstack((self.matrix, np.zeros((self.matrix.shape[0], len(feature_names)))))
        for i in xrange(self.matrix.shape[0]):
            if len(feature_names) == 1:
                key = self.matrix[i, self.col_index.get_index(key_name)]
                if values.has(key):
                    self.matrix[i, -1] = values.get(key)
                else:
                    self.matrix[i, -1] = dft
            else:
                key = self.matrix[i, self.col_index.get_index(key_name)]
                for j in xrange(len(feature_names)):
                    name = feature_names[j]
                    if values.has(key):
                        self.matrix[i, self.col_index.get_index(name)] = values.get(key)[j]
                    else:
                        self.matrix[i, self.col_index.get_index(name)] = dft
    def get_col(self, feature_name):
        return self.matrix[:, self.col_index.get_index(feature_name)]
    def get_cell(self, index, feature_name):
        return self.matrix[index, self.col_index.get_index(feature_name)]
    def row_to_dict(self, i):
        dic = {}
        for name in self.col_names:
            dic[name] = self.matrix[i, self.col_index.get_index(name)]
        return dic
    def select_row(self, condition):
        result = list()
        for i in xrange(self.matrix.shape[0]):
            if condition(self.row_to_dict(i)):
                result.append(transform(self.matrix[i], self.col_index))
        return Matrix(np.array(result), self.col_names, self.col_formats)
    def select_col(self, names):
        indices = [self.col_index.get_index(name) for name in names]
        return Matrix(self.matrix[:, indices], self.col_names[:, indices], self.col_formats[:, indices])
    def cat_col(self, cols, names, formats):
        if cols.shape[1] == 1:
            self.col_index.register(names)
            self.col_names.append(names)
            self.col_formats.append(fmt)
            self.matrix = np.hstack((self.matrix, np.array([cols]).T))
        else:
            for name in names:
                self.col_index.register(name)
                self.col_names.append(name)
            for fmt in formats:
                self.col_formats.append(fmt)
            self.matrix = np.hstack((self.matrix, cols))
    def merge_rows(self, other):
        self.ndata += other.ndata
        self.matrix = np.vstack((self.matrix, other.matrix))
        return self
    def join_op(self, key_name0, key_name1, feature_name, values0, values1, op, fmt):
        self.col_index.register(feature_name)
        self.matrix = np.hstack((self.matrix, np.zeros((self.ndata, 1))))
        self.col_formats.append(fmt)
        self.col_names.append(feature_name)
        for i in xrange(self.ndata):
            key0 = self.matrix[i, self.col_index.get_index(key_name0)]
            key1 = self.matrix[i, self.col_index.get_index(key_name1)]
            if not values0.has(key0) or not values1.has(key1):
                self.matrix[i, -1] = self.default
            else:
                value0 = values0.get(key0)
                value1 = values1.get(key1)
                self.matrix[i, -1] = op(value0, value1)
    def join_by_double_key(self, key_name0, key_name1, feature_name, values, fmt, dft=0.0):
        self.col_index.register(feature_name)
        self.matrix = np.hstack((self.matrix, np.zeros((self.ndata, 1))))
        self.col_formats.append(fmt)
        self.col_names.append(feature_name)
        for i in xrange(self.ndata):
            key0 = self.matrix[i, self.col_index.get_index(key_name0)]
            key1 = self.matrix[i, self.col_index.get_index(key_name1)]
            if values.has(key0):
                value0 = values.get(key0)
                if value0.has(key1):
                    self.matrix[i, -1] = value0.get(key1)
                else:
                    self.matrix[i, -1] = dft
            else:
                self.matrix[i, -1] = dft
    def gen_arith_feature(self, col0, col1, feature_name, op, fmt, dft=0.0):
        self.col_index.register(feature_name)
        self.matrix = np.hstack((self.matrix, np.zeros((self.ndata, 1))))
        self.col_formats.append(fmt)
        self.col_names.append(feature_name)
        for i in xrange(self.ndata):
            value0 = self.matrix[i, self.col_index.get_index(col0)]
            value1 = self.matrix[i, self.col_index.get_index(col1)]
            if value0 == self.default or value1 == self.default:
                self.matrix[i, -1] = dft
            else:
                self.matrix[i, -1] = op(value0, value1)
    def gen_feature(self, feature_name, func, fmt):
        self.col_index.register(feature_name)
        self.matrix = np.hstack((self.matrix, np.zeros((self.ndata, 1))))
        self.col_formats.append(fmt)
        self.col_names.append(feature_name)
        for i in xrange(self.ndata):
            self.matrix[i, self.col_index.get_index(feature_name)] = func(self.row_to_dict(i))
    def gen_features(self, feature_names, func, fmts):
        for name in feature_names:
            self.col_index.register(name)
            self.col_names.append(name)
        self.matrix = np.hstack((self.matrix, np.zeros((self.ndata, len(feature_names)))))
        for fmt in fmts:
            self.col_formats.append(fmt)
        col_indices = []
        for name in feature_names:
            col_indices.append(self.col_index.get_index(name))
        for i in xrange(self.ndata):
            self.matrix[i, col_indices] = func(self.row_to_dict(i))
    def drop(self, feature_name):
        index_to_drop = self.col_index.get_index(feature_name)
        if index_to_drop == 0:
            self.col_names = self.col_names[1:]
            self.col_formats = self.col_formats[1:]
            self.matrix = self.matrix[:, 1:]
        elif index_to_drop == self.matrix.shape[1] - 1:
            self.col_names = self.col_names[:-1]
            self.col_formats = self.col_formats[:-1]
            self.matrix = self.matrix[:, :-1]
        else:
            self.col_names = self.col_names[:index_to_drop] + self.col_names[index_to_drop + 1:]
            self.col_formats = self.col_formats[:index_to_drop] + self.col_formats[index_to_drop + 1:]
            if index_to_drop == 1:
                self.matrix = np.hstack((np.array([self.matrix[:, 0]]).T, self.matrix[:, 2:]))
            elif index_to_drop == self.matrix.shape[1] - 2:
                self.matrix = np.hstack((self.matrix[:, :index_to_drop], np.array([self.matrix[:, -1]]).T))
            else:
                self.matrix = np.hstack((self.matrix[:, :index_to_drop], self.matrix[:, index_to_drop + 1:]))
        self.col_index.drop(feature_name)
    def set(self, row_number, feature_name, value):
        self.matrix[row_number, self.col_index.get_index(feature_name)] = value
    def set_row(self, row_number, values):
        self.matrix[row_number, :] = values
