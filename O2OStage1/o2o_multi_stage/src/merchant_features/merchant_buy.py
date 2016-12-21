import numpy as np
from src.utils.matrix import Matrix
from src.metainfo import *
from src.utils.hashset import HashSet
from src.feature_operations import append_feature_by_month
from src.feature_operations import drop
from src.metainfo import paths
import sklearn

def gen_merchant_buy(X, month):

    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + 'online_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    merchant_buy = HashSet()
    merchant_buy_with_coupon = HashSet()
    merchant_distribute_coupon = HashSet()  
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        if date_str != 'null':
            merchant_buy.add_one(mid_str)
            if cid_str != 'null':
                merchant_buy_with_coupon.add_one(mid_str)
        if cid_str != 'null':
            merchant_distribute_coupon.add_one(mid_str)
    for i in xrange(online.ndata):
        if i % 100000 == 0:
            print i
        mid_str = online.get_cell(i, "mid")
        act_str = online.get_cell(i, "act")
        cid_str = online.get_cell(i, "cid")
        if act_str == '1':
            merchant_buy.add_one(mid_str)
            if cid_str != 'null':
                merchant_buy_with_coupon.add_one(mid_str)
        if cid_str != 'null':
            assert act_str != '0'
            merchant_distribute_coupon.add_one(mid_str)
    X.join("mid", ["merchant_buy"], merchant_buy, ["%s"], 0.0)
    X.join("mid", ["merchant_buy_with_coupon"], merchant_buy_with_coupon, ["%s"], 0.0)
    X.join("mid", ["merchant_distribute_coupon"], merchant_distribute_coupon, ["%s"], 0.0)
    def divide(x, y):
        assert float(x) <= float(y)
        if float(y) == 0:
            return 0.0
        else:
            return float(x) * 1.0 / float(y)
    X.gen_arith_feature("merchant_buy_with_coupon", "merchant_buy", "merchant_buy_with_coupon_ratio", divide, "%s", dft=0.0)
    X.gen_arith_feature("merchant_buy_with_coupon", "merchant_distribute_coupon", "merchant_coupon_ratio", divide, "%s", dft=0.0)
    X.check_point(month)

def gen_merchant_share(X, month):
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + 'online_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    merchant_user_buy = HashSet()
    merchant_user_use_coupon = HashSet()
    merchant_user_buy_counter = HashSet()
    merchant_user_use_coupon_counter = HashSet()
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        mid_str = offline.get_cell(i, "mid")
        uid_str = offline.get_cell(i, "uid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        if date_str != 'null':
            if not merchant_user_buy.has(mid_str):
                merchant_user_buy.set(mid_str, HashSet())
            if not merchant_user_buy.get(mid_str).has(uid_str):
                merchant_user_buy.get(mid_str).add_one(uid_str)
                merchant_user_buy_counter.add_one(mid_str)
            if cid_str != 'null':
                if not merchant_user_use_coupon.has(mid_str):
                    merchant_user_use_coupon.set(mid_str, HashSet())
                if not merchant_user_use_coupon.get(mid_str).has(uid_str):
                    merchant_user_use_coupon.get(mid_str).add_one(uid_str)
                    merchant_user_use_coupon_counter.add_one(mid_str)
    for i in xrange(online.ndata):
        if i % 100000 == 0:
            print i
        mid_str = online.get_cell(i, "mid")
        uid_str = online.get_cell(i, "uid")
        act_str = online.get_cell(i, "act")
        cid_str = online.get_cell(i, "cid")
        if act_str == '1':
            if not merchant_user_buy.has(mid_str):
                merchant_user_buy.set(mid_str, HashSet())
            if not merchant_user_buy.get(mid_str).has(uid_str):
                merchant_user_buy.get(mid_str).add_one(uid_str)
                merchant_user_buy_counter.add_one(mid_str)
            if cid_str != 'null':
                if not merchant_user_use_coupon.has(mid_str):
                    merchant_user_use_coupon.set(mid_str, HashSet())
                if not merchant_user_use_coupon.get(mid_str).has(uid_str):
                    merchant_user_use_coupon.get(mid_str).add_one(uid_str)
                    merchant_user_use_coupon_counter.add_one(mid_str)
    X.join("mid", ["merchant_user_buy"], merchant_user_buy_counter, ["%s"], dft=0.0)
    X.join("mid", ["merchant_user_use_coupon"], merchant_user_use_coupon_counter, ["%s"], dft=0.0)
    X.check_point(month)

if __name__ == '__main__':
    #drop("merchant_user_buy0", 2)
    #for i in xrange(3, 8):
    append_feature_by_month(gen_merchant_buy, 2)
    append_feature_by_month(gen_merchant_share, 2)
    append_feature_by_month(gen_merchant_buy, 3)
    append_feature_by_month(gen_merchant_share, 3)
