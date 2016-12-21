import numpy as np
from code.metainfo import paths
from code.utils.matrix import Matrix


def append_feature_by_month(func, month):
    col_names = np.genfromtxt(paths.my_path + 'X_{0}_{1}{2}_shop_month_col_names.csv'.format(month, 1, month - 1), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X_{0}_{1}{2}_shop_month.csv'.format(month, 1, month - 1), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    func(X, month)
    #X.check_point(month)

def regen_feature_by_month(func, month, feature_name):
    col_names = np.genfromtxt(paths.my_path + 'col_names{0}.csv'.format(month), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X{0}.csv'.format(month), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    X.drop(feature_name)
    func(X, month, feature_name)
    #X.check_point(month)

def drop(feature_name, month, checkpoint=True):
    col_names = np.genfromtxt(paths.my_path + 'col_names{0}.csv'.format(month), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X{0}.csv'.format(month), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    X.drop(feature_name)
    if checkpoint:
        X.check_point(month)

def drop_multiple(feature_names, month):
    col_names = np.genfromtxt(paths.my_path + 'col_names{0}.csv'.format(month), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X{0}.csv'.format(month), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    for name in feature_names:
        X.drop(name)
    X.check_point(month)
