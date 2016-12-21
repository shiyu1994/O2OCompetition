import numpy as np
import code.metainfo.paths as paths
from code.utils.hashset import HashSet
from code.utils.matrix import Matrix
from code.utils.get_month import get_month

def calc_user_shop_month():
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'ccf_offline_stage1_train.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])

    user_shop_month = HashSet()
    shop_month_coupon = {}
    shop_month_coupon_used = {}
    shop_coupon = {}
    shop_coupon_used = {}
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_rec_str = offline.get_cell(i, "date_rec")
        if date_rec_str == 'null':
            continue
        month = get_month(date_rec_str)
        user_shop_month_set = user_shop_month.get(uid_str, HashSet()).get(mid_str, {})
        if month not in user_shop_month_set:
            user_shop_month_set[month] = 1
        else:
            user_shop_month_set[month] += 1

    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_rec_str = offline.get_cell(i, "date_rec")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        if date_rec_str == 'null':
            continue
        assert cid_str != 'null'
        month = get_month(date_rec_str)
        user_shop_month_set = user_shop_month.get(uid_str, HashSet()).get(mid_str, {})

        if mid_str not in shop_coupon:
            shop_coupon[mid_str] = 1
        else:
            shop_coupon[mid_str] += 1
        if date_str != 'null':
            if mid_str not in shop_coupon_used:
                shop_coupon_used[mid_str] = 1
            else:
                shop_coupon_used[mid_str] += 1

        if month in user_shop_month_set and user_shop_month_set[month] > 1:
            if mid_str not in shop_month_coupon:
                shop_month_coupon[mid_str] = 1
            else:
                shop_month_coupon[mid_str] += 1
            if date_str != 'null':
                if mid_str not in shop_month_coupon_used:
                    shop_month_coupon_used[mid_str] = 1
                else:
                    shop_month_coupon_used[mid_str] += 1
    col_names = ["user_shop_month", "shop_coupon", "shop_coupon_used", "shop_month_coupon", "shop_month_coupon_used"]
    statistics = np.zeros((offline.ndata, 5), dtype=float)
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        user_shop_month_set = user_shop_month.get(uid_str, HashSet()).get(mid_str, {})  
        statistics[i, :] = np.array([user_shop_month_set.get(month, 0), shop_coupon.get(mid_str, 0), \
            shop_coupon_used.get(mid_str, 0), shop_month_coupon.get(mid_str, 0), shop_month_coupon_used.get(mid_str, 0)])
    offline.cat_col(statistics, col_names, ["%s" for i in xrange(len(col_names))])
    offline.check_point("shop_month_coupon")

if __name__ == '__main__':
    calc_user_shop_month()
