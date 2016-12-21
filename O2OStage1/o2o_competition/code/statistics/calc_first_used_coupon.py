import numpy as np
from code.utils.matrix import Matrix
from code.utils.hashset import HashSet
import code.metainfo.paths as paths

def calc_first_used(month):
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_test_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    user_coupon = HashSet()
    user_total_used = HashSet()
    user_first_used = HashSet()
    user_first_coupon = HashSet()
    for i in xrange(offline.ndata):
        if i % 10000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        date_rec_str = offline.get_cell(i, "date_rec")
        cid_str = offline.get_cell(i, "cid")
        if cid_str == 'null':
            continue
        user_coupon.add_one(uid_str)

        user_first_coupon_list = user_first_coupon.get(uid_str, HashSet())
        if not user_first_coupon_list.has(mid_str):
            if date_str != 'null':
                user_first_coupon_list.set(mid_str, (date_rec_str, 1))
            else:
                user_first_coupon_list.set(mid_str, (date_rec_str, 0))
        else:
            old_first_date_rec, old_used = user_first_coupon_list.get(mid_str)
            if date_rec_str < old_first_date_rec:
                if date_str != 'null':
                    user_first_coupon_list.set(mid_str, (date_rec_str, 1))
                else:
                    user_first_coupon_list.set(mid_str, (date_rec_str, 0))

        if date_str != 'null':
            user_total_used.add_one(uid_str)

    col_names = ["user_coupon", "user_coupon_used", "user_first_coupon", "user_first_coupon_used", "user_coupon_used_ratio", "user_first_coupon_used_ratio"]
    statistics = np.zeros((offline.ndata, len(col_names)))
    for i in xrange(offline.ndata):
        if i % 10000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        user_first_used_counter = 0
        user_first_counter = 0
        user_first_coupon_list = user_first_coupon.get(uid_str, HashSet())
        for mid in user_first_coupon_list.get_keys():
            user_first_counter += 1
            first_date_rec, used = user_first_coupon_list.get(mid)
            if used == 1:
                user_first_used_counter += 1
        user_coupon_counter = user_coupon.get(uid_str, 0)
        user_total_used_counter = user_total_used.get(uid_str, 0)
        if user_coupon_counter > 0:
            ratio = user_total_used_counter * 1.0 / user_coupon_counter
        else:
            ratio = 0
        if user_first_counter > 0:
            first_ratio = user_first_used_counter * 1.0 / user_first_counter
        statistics[i, :] = [user_coupon_counter, user_total_used_counter, user_first_counter, user_first_used_counter, ratio, first_ratio]
    offline.cat_col(statistics, col_names, ["%s" for i in xrange(len(col_names))])
    offline.check_point("fisrt_use_{0}".format(month)) 

if __name__ == '__main__':
    for i in xrange(2, 7):
        calc_first_used(i)
