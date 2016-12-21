from src.utils.matrix import Matrix
from src.metainfo import *
import numpy as np
from src.utils.feature_index import FeatureIndex
from src.utils.hashset import HashSet
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
        elif re.search(":", dis_rate_str) != None:
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

def gen_user_buy_with_coupon(matrix_offline, matrix_online, X):
    user_buy = HashSet(default=matrix_offline.default)
    user_buy_with_coupon = HashSet(default=matrix_offline.default)
    for i in xrange(matrix_offline.ndata):
        cid_str = matrix_offline.get_cell(i, 'cid')
        date_str = matrix_offline.get_cell(i, 'date')
        uid_str = matrix_offline.get_cell(i, 'uid')
        if date_str != 'null':
            user_buy.add_one(uid_str)
            if cid_str != 'null':
                user_buy_with_coupon.add_one(uid_str)
    for i in xrange(matrix_online.ndata):
        act_str = matrix_online.get_cell(i, 'act')
        cid_str = matrix_online.get_cell(i, 'cid')
        uid_str = matrix_online.get_cell(i, 'uid')
        if act_str == '1':
            user_buy.add_one(uid_str)
            if cid_str != 'null':
                user_buy_with_coupon.add_one(uid_str)
    user_buy_with_coupon_freq = user_buy.merge_op(user_buy_with_coupon, lambda x, y: float(y) * 1.0 / float(x), dft=0.0)
    X.join("uid", ["user_buy", "user_buy_with_coupon", "user_buy_with_coupon_freq"], user_buy.merge(user_buy_with_coupon, dft=0.0).merge(user_buy_with_coupon_freq, dft=0.0), ("%s" for i in xrange(3)), dft=0.0)

def gen_user_buy_in_shop(matrix_offline, matrix_online, X):
    user_buy_in_shop = HashSet(default=matrix_offline.default)
    for i in xrange(matrix_offline.ndata):
        date_str = matrix_offline.get_cell(i, 'date')
        uid_str = matrix_offline.get_cell(i, 'uid')
        mid_str = matrix_offline.get_cell(i, "mid")
        if date_str != 'null':
            if not user_buy_in_shop.has(uid_str):
                user_buy_in_shop.set(uid_str, HashSet())
            user_buy_in_shop.get(uid_str).add_one(mid_str)
    for i in xrange(matrix_online.ndata):
        act_str = matrix_online.get_cell(i, 'act')
        uid_str = matrix_online.get_cell(i, 'uid')
        mid_str = matrix_online.get_cell(i, "mid")
        if act_str == '1':
            if not user_buy_in_shop.has(uid_str):
                user_buy_in_shop.set(uid_str, HashSet())
            user_buy_in_shop.get(uid_str).add_one(mid_str)
    X.join_by_double_key("uid", "mid", "user_buy_in_shop", user_buy_in_shop, "%s", dft=0.0)
    def divide(x, y):
        assert float(x) <= float(y)
        if float(y) == 0:
            return 0
        else:
            return float(x) * 1.0 / float(y)
    X.gen_arith_feature("user_buy_in_shop", "user_buy", "user_buy_in_shop_ratio", divide, fmt="%s", dft=0.0)


def gen_user_shop_features(path_to_user_features_csv, path_to_shop_features_csv, matrix):
    ufeatures = HashSet(np.genfromtxt(path_to_user_features_csv, delimiter=',', dtype=float))
    sfeatures = HashSet(np.genfromtxt(path_to_shop_features_csv, delimiter=',', dtype=float))
    matrix.join_op("uid", "mid", "score0", ufeatures, sfeatures, lambda x, y: np.sum(np.array(x) * np.array(y)), "%s")
    matrix.join("uid", ["user_cofi_features{0}".format(i) for i in xrange(ufeatures.row_dim)], ufeatures, ["%s" for i in xrange(ufeatures.row_dim)])
    matrix.join("mid", ["shop_cofi_features{0}".format(i) for i in xrange(sfeatures.row_dim)], sfeatures, ["%s" for i in xrange(sfeatures.row_dim)])

def gen_user_shop_tags(path_to_user_tags_csv, path_to_shop_tags_csv, matrix):
    utags = HashSet(np.genfromtxt(path_to_user_tags_csv, delimiter=',', dtype=float))
    stags = HashSet(np.genfromtxt(path_to_shop_tags_csv, delimiter=',', dtype=float))
    matrix.join("uid", ["u_tag_{0}".format(i) for i in xrange(utags.row_dim)], utags, ["%s" for i in xrange(utags.row_dim)])
    matrix.join("mid", ["m_tag_{0}".format(i) for i in xrange(stags.row_dim)], stags, ["%s" for i in xrange(stags.row_dim)])

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

def gen_user_buy_coupon_in_shop(matrix_offline, matrix_online, X):
    user_buy_with_coupon_in_shop = HashSet()
    user_buy_without_coupon_in_shop = HashSet()
    for i in xrange(matrix_offline.ndata):
        uid_str = matrix_offline.get_cell(i, "uid")
        cid_str = matrix_offline.get_cell(i, "cid")
        date_str = matrix_offline.get_cell(i, "date")
        mid_str = matrix_offline.get_cell(i, "mid")
        if date_str != 'null':
            if not user_buy_with_coupon_in_shop.has(uid_str):
                user_buy_with_coupon_in_shop.set(uid_str, HashSet())
                user_buy_without_coupon_in_shop.set(uid_str, HashSet())
            if cid_str != 'null':
                user_buy_with_coupon_in_shop.get(uid_str).add_one(mid_str)
            else:
                user_buy_without_coupon_in_shop.get(uid_str).add_one(mid_str)

    for i in xrange(matrix_online.ndata):
        uid_str = matrix_online.get_cell(i, "uid")
        cid_str = matrix_online.get_cell(i, "cid")
        mid_str = matrix_online.get_cell(i, "mid")
        act_str = matrix_online.get_cell(i, "act")
        if act_str == '1':
            if not user_buy_with_coupon_in_shop.has(uid_str):
                user_buy_with_coupon_in_shop.set(uid_str, HashSet())
                user_buy_without_coupon_in_shop.set(uid_str, HashSet())
            if cid_str != 'null':
                user_buy_with_coupon_in_shop.get(uid_str).add_one(mid_str)
            else:
                user_buy_without_coupon_in_shop.get(uid_str).add_one(mid_str)

    X.join_by_double_key("uid", "mid", "user_buy_with_coupon_in_shop", user_buy_with_coupon_in_shop, "%s", dft=0.0)
    X.join_by_double_key("uid", "mid", "user_buy_without_coupon_in_shop", user_buy_without_coupon_in_shop, "%s", dft=0.0)
    def divide(x, y):
        assert float(x) <= float(y)
        if float(y) == 0:
            return 0
        else:
            return float(x) * 1.0 / float(y)
    X.gen_arith_feature("user_buy_with_coupon_in_shop", "user_buy_in_shop", "user_buy_with_coupon_in_shop_ratio", divide, "%s", dft=0.0)
    X.gen_arith_feature("user_buy_without_coupon_in_shop", "user_buy_in_shop", "user_buy_without_coupon_in_shop_ratio", divide, "%s", dft=0.0)

def gen_user_get_coupon(offline_source, online_source, X, month):

    offline = Matrix(np.genfromtxt(paths.ccf_path + offline_source, delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + online_source, delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    user_get_coupon = HashSet()
    for i in xrange(offline.ndata):
        uid_str = offline.get_cell(i, "uid")
        cid_str = offline.get_cell(i, "cid")
        if cid_str != 'null':
            user_get_coupon.add_one(uid_str)
    for i in xrange(online.ndata):
        uid_str = online.get_cell(i, "uid")
        cid_str = online.get_cell(i, "cid")
        act_str = online.get_cell(i, "act")
        if cid_str != 'null':
            assert act_str != '0'
            user_get_coupon.add_one(uid_str)
    X.join("uid", ["user_get_coupon"], user_get_coupon, ["%s"], dft=0.0)

    def divide(x, y):
        assert float(x) != -9999 and float(y) != -9999
        assert float(x) <= float(y), "{0} {1}".format(x, y)
        if float(y) == 0:
            return 0.0
        else:
            return float(x) * 1.0 / float(y)
    X.gen_arith_feature("user_buy_with_coupon", "user_get_coupon", "user_use_coupon_freq", divide, "%s", dft=0.0)

def gen_user_get_shop_coupon(offline_source, online_source, X):
    offline = Matrix(np.genfromtxt(paths.ccf_path + offline_source, delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + online_source, delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    user_get_shop_coupon = HashSet()
    for i in xrange(offline.ndata):
        uid_str = offline.get_cell(i, "uid")
        cid_str = offline.get_cell(i, "cid")
        mid_str = offline.get_cell(i, "mid")
        if cid_str != 'null':
            if not user_get_shop_coupon.has(uid_str):
                user_get_shop_coupon.set(uid_str, HashSet())
            user_get_shop_coupon.get(uid_str).add_one(mid_str)
    for i in xrange(online.ndata):
        uid_str = online.get_cell(i, "uid")
        cid_str = online.get_cell(i, "cid")
        mid_str = online.get_cell(i, "mid")
        act_str = online.get_cell(i, "act")
        if cid_str != 'null' and act_str != '0':
            if not user_get_shop_coupon.has(uid_str):
                user_get_shop_coupon.set(uid_str, HashSet())
            user_get_shop_coupon.get(uid_str).add_one(mid_str)
    X.join_by_double_key("uid", "mid", "user_get_shop_coupon", user_get_shop_coupon, "%s", dft=0.0)
    def divide(x, y):
        assert float(x) <= float(y)
        if float(y) == 0:
            return 0.0
        else:
            return float(x) * 1.0 / float(y)
    X.gen_arith_feature("user_buy_with_coupon_in_shop", "user_get_shop_coupon", "user_use_shop_coupon_freq", divide, "%s", dft=0.0)

def gen_unused_shop_coupon(X):
    def minus(x, y):
        assert float(x) >= float(y)
        return float(x) - float(y)
    X.gen_arith_feature("user_get_shop_coupon", "user_buy_with_coupon_in_shop", "user_unused_shop_coupon", minus, "%s", dft=0.0)

def gen_no_penalty_user_shop_features(X, month):
    ufeatures = HashSet(np.genfromtxt(paths.my_path + 'u_features_no_penalty_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    sfeatures = HashSet(np.genfromtxt(paths.my_path + 'i_features_no_penalty_10_{0}.csv'.format(month), delimiter=',', dtype=float))
    X.join_op("uid", "mid", "score1", ufeatures, sfeatures, lambda x, y: np.sum(np.array(x) * np.array(y)), "%s")
    X.join("uid", ["user_cofi_features_no_penalty{0}".format(i) for i in xrange(ufeatures.row_dim)], ufeatures, ["%s" for i in xrange(ufeatures.row_dim)])
    X.join("mid", ["shop_cofi_features_no_penalty{0}".format(i) for i in xrange(sfeatures.row_dim)], sfeatures, ["%s" for i in xrange(sfeatures.row_dim)])

def append_feature_by_month(func, month, since, to):
    col_names = np.genfromtxt(paths.my_path + 'col_names_{0}_{1}{2}.csv'.format(month, since, to), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X_{0}_{1}{2}.csv'.format(month, since, to), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    func(X, month)
    X.check_point(month, since, to)

def regen_feature_by_month(func, month, feature_name, fname):
    col_names = np.genfromtxt(paths.my_path + 'col_names{0}.csv'.format(month), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X{0}.csv'.format(month), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    X.drop(feature_name)
    func(X, month, feature_name)
    #X.check_point(fname)

def drop(feature_name, month, fname):
    col_names = np.genfromtxt(paths.my_path + 'col_names{0}.csv'.format(month), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X{0}.csv'.format(month), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])
    X.drop(feature_name)
    #X.check_point(fname)

def gen_by_month(offline_source, online_source, target_file, month, since=None):

    offline = Matrix(np.genfromtxt(paths.ccf_path + offline_source, delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + online_source, delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    if month < 7:
        X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_test_' + str(month) + '.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    else:
        X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_test_' + str(month) + '.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec"], ["%s" for i in xrange(6)])
    print "generating month {0}".format(month)
    print "gen_basic_features"  #11
    gen_basic_features(X)
    print "gen_user_buy_with_coupon"
    gen_user_buy_with_coupon(offline, online, X) #14
    print "gen_user_shop_features"
    #X.join("uid", feature_names, features, formats, dft=0.0)
    gen_user_shop_features(paths.my_path + "u_features_10_{0}.csv".format(month), paths.my_path + "i_features_10_{0}.csv".format(month), X) #35
    print "gen_user_buy_in_shop"
    gen_user_buy_in_shop(offline, online, X) #37

    print "gen_user_buy_coupon_in_shop"
    gen_user_buy_coupon_in_shop(offline, online, X)
    if month < 7:
        print "gen_label"
        gen_label(X)

    print "gen_user_get_shop_coupon"
    gen_user_get_shop_coupon(offline_source, online_source, X)

    print "gen_unused_shop_coupon"
    gen_unused_shop_coupon(X)

    print "gen_no_penalty_user_shop_features"
    gen_no_penalty_user_shop_features(X, month)

    X.check_point(target_file)

if __name__ == '__main__':
    for i in xrange(3, 7):
        print 1, i
        #gen_by_month('offline_1_month{0}.csv'.format(i - 1), 'online_1_month{0}.csv'.format(i - 1), 'X_{0}_{1}{2}.csv'.format(i, i - 1, i - 1), i)
        append_feature_by_month(lambda X, month: gen_user_get_coupon('offline_1_month{0}.csv'.format(i - 1), 'online_1_month{0}.csv'.format(i - 1), X, month), i, i - 1, i - 1)
    for i in xrange(4, 7):
        print 2, i
        #gen_by_month('offline_2_month{0}{1}.csv'.format(i - 2, i - 1), 'online_2_month{0}{1}.csv'.format(i - 2, i - 1), 'X_{0}_{1}{2}.csv'.format(i, i - 2, i - 1), i)
        append_feature_by_month(lambda X, month: gen_user_get_coupon('offline_2_month{0}{1}.csv'.format(i - 2, i - 1), 'online_2_month{0}{1}.csv'.format(i - 2, i - 1), X, month), i, i - 2, i - 1)
    for i in xrange(5, 7):
        print 3, i
        #gen_by_month('offline_3_month{0}{1}.csv'.format(i - 3, i - 1), 'online_3_month{0}{1}.csv'.format(i - 3, i - 1), 'X_{0}_{1}{2}.csv'.format(i, i - 3, i - 1), i)
        append_feature_by_month(lambda X, month: gen_user_get_coupon('offline_3_month{0}{1}.csv'.format(i - 3, i - 1), 'online_3_month{0}{1}.csv'.format(i - 3, i - 1), X, month), i, i - 3, i - 1)
    for i in xrange(6, 7):
        print 4, i
        #gen_by_month('offline_4_month{0}{1}.csv'.format(i - 4, i - 1), 'online_4_month{0}{1}.csv'.format(i - 4, i - 1), 'X_{0}_{1}{2}.csv'.format(i, i - 4, i - 1), i)
        append_feature_by_month(lambda X, month: gen_user_get_coupon('offline_4_month{0}{1}.csv'.format(i - 4, i - 1), 'online_4_month{0}{1}.csv'.format(i - 4, i - 1), X, month), i, i - 4, i - 1)
