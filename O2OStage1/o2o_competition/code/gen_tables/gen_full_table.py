import numpy as np
from code.metainfo import paths as paths
from code.utils.matrix import Matrix
from code.utils.hashset import HashSet
from code.utils.get_month import get_month

def get_month(date_str):
    date_int = int(date_str)
    year = date_int / 10000
    month = date_int / 100 % 100
    date = date_int % 100
    return int((np.datetime64('%d-%02d' % (year, month)) - np.datetime64('2016-01')) / np.timedelta64(1, 'M') + 1)

def gen_act_counts_by_month(month):
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_1_month{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + 'online_1_month{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])
    user_hash_set = HashSet()
    full_hash_set = HashSet()
    ndata = 0
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        cid_str = offline.get_cell(i, "cid")
        date_str = offline.get_cell(i, "date")
        date_rec_str = offline.get_cell(i, "date_rec")
        if date_str != 'null':
            act_counts = full_hash_set.get(uid_str, HashSet()).get(mid_str, np.zeros(3, dtype=float))
            user_act_counts = user_hash_set.get(uid_str, np.zeros(3, dtype=float))
            if act_counts[0] == 0 and act_counts[1] == 0 and act_counts[2] == 0:
                ndata += 1
            if cid_str != 'null':
                act_counts[2] += 1
                user_act_counts[2] += 1
            else:
                act_counts[1] += 1
                user_act_counts[1] += 1
        elif date_rec_str != 'null':
            act_counts = full_hash_set.get(uid_str, HashSet()).get(mid_str, np.zeros(3, dtype=float))
            user_act_counts = user_hash_set.get(uid_str, np.zeros(3, dtype=float))
            if act_counts[0] == 0 and act_counts[1] == 0 and act_counts[2] == 0:
                ndata += 1
            act_counts[0] += 1
            user_act_counts[0] += 1
    for i in xrange(online.ndata):
        if i % 100000 == 0:
            print i
        uid_str = online.get_cell(i, "uid")
        mid_str = online.get_cell(i, "mid")
        cid_str = online.get_cell(i, "cid")
        date_str = online.get_cell(i, "date")
        date_rec_str = online.get_cell(i, "date_rec")
        act_str = online.get_cell(i, "act")

        if date_str != 'null' and act_str == '1':
            act_counts = full_hash_set.get(uid_str, HashSet()).get(mid_str, np.zeros(3, dtype=float))
            user_act_counts = user_hash_set.get(uid_str, np.zeros(3, dtype=float))
            if act_counts[0] == 0 and act_counts[1] == 0 and act_counts[2] == 0:
                ndata += 1
            if cid_str != 'null':
                act_counts[2] += 1
                user_act_counts[2] += 1
            else:
                act_counts[1] += 1
                user_act_counts[1] += 1
        elif date_rec_str != 'null':
            assert act_str != '0'
            act_counts = full_hash_set.get(uid_str, HashSet()).get(mid_str, np.zeros(3, dtype=float))
            user_act_counts = user_hash_set.get(uid_str, np.zeros(3, dtype=float))
            if act_counts[0] == 0 and act_counts[1] == 0 and act_counts[2] == 0:
                ndata += 1
            act_counts[0] += 1
            user_act_counts[0] += 1
    col_names = ["uid", "mid", \
        "unused_coupon", "buy_without_coupon", "use_coupon", \
        "total_coupon", "total_buy", \
        "act_ratio_0", "act_ratio_1", "act_ratio_2", \
        "used_ratio", "unused_ratio", "buy_with_coupon_ratio", "buy_without_coupon_ratio", \
        "unused_coupon_shop_ratio", "buy_without_coupon_shop_ratio", "use_coupon_shop_ratio"]
    full_table = Matrix(np.zeros((ndata, len(col_names))), col_names, col_formats=["%s" for i in xrange(len(col_names))])
    row_index = 0
    for uid in full_hash_set.get_keys():
        user_act_counts = user_hash_set.get(uid)
        user_total_unused_coupon = user_act_counts[0]
        user_total_buy_without_coupon = user_act_counts[1]
        user_total_use_coupon = user_act_counts[2]
        for mid in full_hash_set.get(uid).get_keys():
            if row_index % 100000 == 0:
                print row_index
            acts = full_hash_set.get(uid).get(mid)
            use_coupon = acts[2]
            unused_coupon = acts[0]
            buy_without_coupon = acts[1]
            total_acts = use_coupon + unused_coupon + buy_without_coupon
            total_coupon = use_coupon + unused_coupon
            total_buy = use_coupon + buy_without_coupon
            def divide(x, y):
                if y == 0:
                    return 0
                else:
                    return x * 1.0 / y
            full_table.set_row(row_index, np.array([uid, mid, unused_coupon, buy_without_coupon, use_coupon, \
                total_coupon, total_buy, \
                divide(unused_coupon, total_acts), divide(buy_without_coupon, total_acts), divide(use_coupon, total_acts), \
                divide(use_coupon, total_coupon), divide(unused_coupon, total_coupon), \
                divide(use_coupon, total_buy), divide(buy_without_coupon, total_buy), \
                divide(unused_coupon, user_total_unused_coupon), divide(buy_without_coupon, user_total_buy_without_coupon), divide(use_coupon, user_total_use_coupon)]))
            row_index += 1
    full_table.check_point("act_counts_month_{0}".format(month))

if __name__ == '__main__':
    for i in xrange(3, 7):
        gen_act_counts_by_month(i)
