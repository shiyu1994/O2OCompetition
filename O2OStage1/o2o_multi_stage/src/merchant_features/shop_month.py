import numpy as np
import src.metainfo.paths as paths
from src.utils.hashset import HashSet
from src.utils.matrix import Matrix
from src.utils.get_month import get_month
from src.feature_operations import append_feature_by_month

def gen_user_shop_month(X, month):
    user_shop_month = HashSet()
    shop_month_coupon = {}
    shop_month_coupon_used = {} 
    shop_coupon = {}
    shop_coupon_used = {}
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        date_rec_str = X.get_cell(i, "date_rec")
        if date_rec_str == 'null':
            continue
        month = get_month(date_rec_str)
        user_shop_month_set = user_shop_month.get(uid_str, HashSet()).get(mid_str, {})
        if month not in user_shop_month_set:
            user_shop_month_set[month] = 1
        else:
            user_shop_month_set[month] += 1

    """
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        date_rec_str = X.get_cell(i, "date_rec")
        date_str = X.get_cell(i, "date")
        cid_str = X.get_cell(i, "cid")
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
    """
    #col_names = ["user_shop_month_11_2", "shop_coupon_11_2", "shop_coupon_used_11_2", "shop_month_coupon_11_2", "shop_month_coupon_used_11_2", "shop_coupon_used_ratio_11_2", "is_shop_coupon_11_2"]
    col_names = ["user_shop_month_11_2", "is_shop_coupon_11_2"]
    statistics = np.zeros((X.ndata, len(col_names)), dtype=float)
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        user_shop_month_set = user_shop_month.get(uid_str, HashSet()).get(mid_str, {})
        #if shop_month_coupon.get(mid_str, 0) == 0:
        #    ratio = 0
        #else:
        #    ratio = shop_month_coupon_used.get(mid_str, 0) * 1.0 / shop_month_coupon.get(mid_str)
        if user_shop_month_set.get(month, 0) > 1:
            is_shop_coupon = 1
        else:
            is_shop_coupon = 0
        statistics[i, :] = np.array([user_shop_month_set.get(month, 0), is_shop_coupon])
    X.cat_col(statistics, col_names, ["%s" for i in xrange(len(col_names))])
    X.check_point("X_{0}_{1}{2}_shop_month".format(month, 1, month - 1))

if __name__ == '__main__':
    for month in xrange(7, 8):
        append_feature_by_month(gen_user_shop_month, month)
