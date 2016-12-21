import numpy as np
from code.utils.matrix import Matrix
import code.metainfo.paths as paths
from code.utils.hashset import HashSet
import re

def gen_basic_features(matrix):
    dis_rates = np.zeros(matrix.ndata)
    dis_counts = np.zeros(matrix.ndata)
    lower_bounds = np.zeros(matrix.ndata)
    dists = np.zeros(matrix.ndata)
    for i in xrange(matrix.ndata):
        dis_rate_str = matrix.get_cell(i, 'dis_rate')
        if dis_rate_str == 'null' or dis_rate_str == 'fixed':
            dis_rates[i] = matrix.default
            lower_bounds[i] = matrix.default
            dis_counts[i] = matrix.default
        elif re.search(":", dis_rate_str) is not None:
            nums = re.split(":", dis_rate_str)
            lower_bounds[i] = int(nums[0])
            dis_counts[i] = int(nums[1])
            dis_rates[i] = 1.0 - int(nums[1]) * 1.0 / int(nums[0])
        else:
            dis_rates[i] = float(dis_rate_str)
            lower_bounds[i] = 0.0
            dis_counts[i] = matrix.default
        dist_str = matrix.get_cell(i, 'dist')
        if dist_str == 'null':
            dists[i] = matrix.default
        else:
            dists[i] = int(dist_str)
    matrix.cat_col(np.vstack((dis_rates, dis_counts, lower_bounds, dists)).T, ["dis_rate", "lower_bound", "dis_count", "dist"], ["%s" for i in xrange(4)])

def gen_user_shop_features(X, month):
    ufeatures = HashSet(np.genfromtxt(paths.my_path + 'u_features_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    sfeatures = HashSet(np.genfromtxt(paths.my_path + 'i_features_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    X.join_op("uid", "mid", "score0", ufeatures, sfeatures, lambda x, y: np.sum(np.array(x) * np.array(y)), "%s")
    X.join("uid", ["user_cofi_features{0}".format(i) for i in xrange(ufeatures.row_dim)], ufeatures, ["%s" for i in xrange(ufeatures.row_dim)])
    X.join("mid", ["shop_cofi_features{0}".format(i) for i in xrange(sfeatures.row_dim)], sfeatures, ["%s" for i in xrange(sfeatures.row_dim)])

def gen_no_penalty_user_shop_features(X, month):
    ufeatures = HashSet(np.genfromtxt(paths.my_path + 'u_features_no_penalty_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    sfeatures = HashSet(np.genfromtxt(paths.my_path + 'i_features_no_penalty_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    X.join_op("uid", "mid", "score1", ufeatures, sfeatures, lambda x, y: np.sum(np.array(x) * np.array(y)), "%s")
    X.join("uid", ["user_cofi_features_no_penalty{0}".format(i) for i in xrange(ufeatures.row_dim)], ufeatures, ["%s" for i in xrange(ufeatures.row_dim)])
    X.join("mid", ["shop_cofi_features_no_penalty{0}".format(i) for i in xrange(sfeatures.row_dim)], sfeatures, ["%s" for i in xrange(sfeatures.row_dim)])

def attach_full_act_history(X, month):
    for k in xrange(month - 1, 0, -1):
        print k
        col_names_history = np.genfromtxt(paths.my_path + 'act_counts_month_{0}_col_names.csv'.format(k), delimiter=',', dtype=str)
        history = Matrix(np.genfromtxt(paths.my_path + 'act_counts_month_{0}.csv'.format(k), delimiter=',', dtype=float), col_names_history[0, :], ["%s" for i in xrange(col_names_history.shape[1])])
        history_hash = HashSet()
        for i in xrange(history.ndata):
            if i % 100000 == 0:
                print i
            uid = history.get_cell(i, "uid")
            mid = history.get_cell(i, "mid")
            history_hash.get(uid, HashSet()).set(mid, history.matrix[i, 3:])
        X_history = np.zeros((X.ndata, col_names_history.shape[1] - 3))
        for i in xrange(X.ndata):
            if i % 100000 == 0:
                print i
            uid = X.get_cell(i, "uid")
            mid = X.get_cell(i, "mid")
            X_history[i, :] = history_hash.get(uid, HashSet()).get(mid, np.zeros(col_names_history.shape[1] - 3))
        col_names = []
        for name in col_names_history[0, 3:]:
            col_names.append('{0}_month_{1}'.format(name, month - k))
        X.cat_col(X_history, col_names, ["%s" for i in xrange(len(col_names))])
    for k in xrange(7 - month):
        col_names = []
        for name in col_names_history[0, 3:]:
            col_names.append('{0}_month_{1}'.format(name, month - k))
        X_history = np.zeros((X.ndata, len(col_names)))
        X.cat_col(X_history, col_names, ["%s" for i in xrange(len(col_names))])

def attach_user_act_history(X, month):
    for k in xrange(month - 1, 0, -1):
        print k
        col_names_history = np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}_col_names.csv'.format(k), delimiter=',', dtype=str)
        history = Matrix(np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}.csv'.format(k), delimiter=',', dtype=float), col_names_history[0, :], ["%s" for i in xrange(col_names_history.shape[1])])
        history_hash = HashSet()
        for i in xrange(history.ndata):
            if i % 100000 == 0:
                print i
            uid = history.get_cell(i, "uid")
            history_hash.set(uid, history.matrix[i, 2:])
        X_history = np.zeros((X.ndata, col_names_history.shape[1] - 2))
        for i in xrange(X.ndata):
            if i % 100000 == 0:
                print i
            uid = X.get_cell(i, "uid")
            X_history[i, :] = history_hash.get(uid, np.zeros(col_names_history.shape[1] - 2))
        col_names = []
        for name in col_names_history[0, 2:]:
            col_names.append('{0}_user_month_{1}'.format(name, month - k))
        X.cat_col(X_history, col_names, ["%s" for i in xrange(len(col_names))])
    for k in xrange(7 - month):
        col_names = []
        for name in col_names_history[0, 2:]:
            col_names.append('{0}_user_month_{1}'.format(name, month - k))
        X_history = np.zeros((X.ndata, len(col_names)))
        X.cat_col(X_history, col_names, ["%s" for i in xrange(len(col_names))])

def attach_full_act(X, month):
    col_names = np.genfromtxt(paths.my_path + 'act_counts_month_{0}{1}_col_names.csv'.format(1, month - 1), delimiter=',', dtype=str)
    full_act = Matrix(np.genfromtxt(paths.my_path + 'act_counts_month_{0}{1}.csv'.format(1, month - 1), delimiter=',', dtype=float), col_names[0, :], col_formats=["%s" for i in xrange(col_names.shape[1] - 3)])
    full_act_hash = HashSet()
    for i in xrange(full_act.ndata):
        if i % 100000 == 0:
            print i
        uid = full_act.get_cell(i, "uid")
        mid = full_act.get_cell(i, "mid")
        full_act_hash.get(uid, HashSet()).set(mid, full_act.matrix[i, 3:])
    X_full_act = np.zeros((X.ndata, col_names.shape[1] - 3))
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid = X.get_cell(i, "uid")
        mid = X.get_cell(i, "mid")
        X_full_act[i, :] = full_act_hash.get(uid, HashSet()).get(mid, np.zeros(col_names.shape[1] - 3))

    col_names_check_point = []
    for name in col_names[0, 3:]:
        col_names_check_point.append('{0}_all_month'.format(name))
    X.cat_col(X_full_act, col_names_check_point, ["%s" for i in xrange(len(col_names_check_point))])

def attach_user_act(X, month):
    col_names = np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}{1}_col_names.csv'.format(1, month - 1), delimiter=',', dtype=str)
    full_act = Matrix(np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}{1}.csv'.format(1, month - 1), delimiter=',', dtype=float), col_names[0, :], col_formats=["%s" for i in xrange(col_names.shape[1] - 1)])
    full_act_hash = HashSet()
    for i in xrange(full_act.ndata):
        if i % 100000 == 0:
            print i
        uid = full_act.get_cell(i, "uid")
        full_act_hash.set(uid, full_act.matrix[i, 1:])
    X_full_act = np.zeros((X.ndata, col_names.shape[1] - 1))
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid = X.get_cell(i, "uid")
        X_full_act[i, :] = full_act_hash.get(uid, np.zeros(col_names.shape[1] - 1))
    col_names_check_point = []
    for name in col_names[0, 1:]:
        col_names_check_point.append('{0}_user_all_month'.format(name))
    X.cat_col(X_full_act, col_names_check_point, ["%s" for i in xrange(len(col_names_check_point))])

def gen_label(X):
    def gen_y(dic):
        cid_str = dic['cid']
        assert cid_str != 'null'
        date_str = dic['date']
        if date_str != 'null':
            return 1
        else:
            return 0
    X.gen_feature('y', gen_y, "%s")

def gen_by_month(target_file, month, since=None):
    if month < 7:
        X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_test_' + str(month) + '.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    else:
        X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_test_' + str(month) + '.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec"], ["%s" for i in xrange(6)])

    print "generating month {0}".format(month)

    print "gen_user_shop_features"
    gen_user_shop_features(X, month)

    print "gen_no_penalty_user_shop_features"
    gen_no_penalty_user_shop_features(X, month)

    print "gen_basic_features"
    gen_basic_features(X)

    print "attach_full_act_history"
    attach_full_act_history(X, month)

    print "attach_user_act_history"
    attach_user_act_history(X, month)

    print "attach_full_act"
    attach_full_act(X, month)

    print "attach_user_act"
    attach_user_act(X, month)

    if month < 7:
        print "gen_label"
        gen_label(X)

    X.check_point(target_file)

if __name__ == '__main__':
    #for i in xrange(2, 8):
    #    gen_by_month(i)
    for i in xrange(4, 8):
        gen_by_month("X{0}".format(i), i)
