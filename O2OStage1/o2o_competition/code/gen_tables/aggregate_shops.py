from code.utils.matrix import Matrix
from code.utils.hashset import HashSet
import code.metainfo.paths as paths
import numpy as np

def aggregate_shops_by_month(month):
    def divide(x, y):
        if y == 0:
            return 0
        else:
            return x * 1.0 / y
    col_names = np.genfromtxt(paths.my_path + 'act_counts_month_{0}_col_names.csv'.format(month), delimiter=',', dtype=str)
    full_act_counts = Matrix(np.genfromtxt(paths.my_path + 'act_counts_month_{0}.csv'.format(month), delimiter=',', dtype=float), col_names[0, :], col_formats=["%s" for i in xrange(col_names.shape[1])])
    user_act_counts = HashSet()
    for i in xrange(full_act_counts.ndata):
        uid_str = full_act_counts.get_cell(i, "uid")
        user_acts = user_act_counts.get(uid_str, {"unused_coupons":[], "buy_without_coupons":[], "use_coupons":[], "total_coupons":[], "total_buys":[]})
        user_acts["unused_coupons"].append(full_act_counts.get_cell(i, "unused_coupon"))
        user_acts["buy_without_coupons"].append(full_act_counts.get_cell(i, "buy_without_coupon"))
        user_acts["use_coupons"].append(full_act_counts.get_cell(i, "use_coupon"))
        user_acts["total_coupons"].append(full_act_counts.get_cell(i, "total_coupon"))
        user_acts["total_buys"].append(full_act_counts.get_cell(i, "total_buy"))

    all_users = user_act_counts.get_keys()
    user_col_names = ["uid", \
        "unused_coupon", "buy_without_coupon", "use_coupon", "total_coupon", "total_buy", \
        "act_ratio_0", "act_ratio_1", "act_ratio2", \
        "used_ratio", "unused_ratio", "buy_with_coupon_ratio", "buy_without_coupon_ratio", \
        "unused_coupon_shop_number", "buy_without_coupon_shop_number", "use_coupon_shop_number", "pure_unused_coupon_shop_number", "pure_no_unused_coupon_shop_number", \
        "max_unused_coupon_shop", "max_buy_without_coupon_shop", "max_use_coupon_shop", "max_total_coupon_shop", "max_total_buy_shop", \
        "avg_unused_coupon_shop", "avg_buy_without_coupon_shop", "avg_use_coupon_shop", "avg_total_coupon_shop", "avg_total_buy_shop", \
        "avg_unused_coupon_shop_nonzero", "avg_buy_without_coupon_shop_nonzero", "avg_use_coupon_shop_nonzero", "avg_total_coupon_nonzero", "avg_total_buy_nonzero", \
        "var_unused_coupon_shop", "var_buy_without_coupon_shop", "var_use_coupon_shop", "var_total_coupon_shop", "var_total_buy_shop", \
        "var_unused_coupon_shop_nonzero", "var_buy_without_coupon_shop_nonzero", "var_use_coupon_shop_nonzero", "var_total_coupon_shop_nonzero", "var_total_buy_shop_nonzero", \
        "mid_unused_coupon_shop_nonzero", "mid_buy_without_coupon_shop_nonzero", "mid_use_coupon_shop_nonzero", "mid_total_coupon_shop_nonzero", "mid_total_buy_shop_nonzero", \
        "min_unused_coupon_shop_nonzero", "min_buy_without_coupon_shop_nonzero", "min_use_coupon_shop_nonzero", "min_total_coupon_shop_nonzero", "min_total_buy_shop_nonzero"]
    user_table = Matrix(np.zeros((len(all_users), len(user_col_names))), user_col_names, col_formats=["%s" for i in xrange(len(user_col_names))])
    row_index = 0
    for uid in user_act_counts.get_keys():
        if row_index % 10000 == 0:
            print row_index

        user_acts = user_act_counts.get(uid)

        unused_coupons = np.array(user_acts["unused_coupons"])
        buy_without_coupons = np.array(user_acts["buy_without_coupons"])
        use_coupons = np.array(user_acts["use_coupons"])
        total_coupons = np.array(user_acts['total_coupons'])
        total_buys = np.array(user_acts["total_buys"])

        unused_coupon = np.sum(unused_coupons)
        buy_without_coupon = np.sum(buy_without_coupons)
        use_coupon = np.sum(use_coupons)
        total_coupon = np.sum(total_coupons)
        total_buy = np.sum(total_buys)
        total_acts = unused_coupon + buy_without_coupon + use_coupon

        act_ratio_0 = divide(unused_coupon, total_acts)
        act_ratio_1 = divide(buy_without_coupon, total_acts)
        act_ratio_2 = divide(use_coupon, total_acts)

        used_ratio = divide(use_coupon, total_coupon)
        unused_ratio = divide(unused_coupon, total_coupon)
        buy_with_coupon_ratio = divide(use_coupon, total_buy)
        buy_without_coupon_ratio = divide(buy_without_coupon, total_buy)

        unused_coupon_shop_number = np.sum(unused_coupons > 0)
        buy_without_coupon_shop_number = np.sum(buy_without_coupons > 0)
        use_coupon_shop_number = np.sum(use_coupons > 0)
        pure_unused_coupon_shop_number = len(unused_coupons) - use_coupon_shop_number
        pure_no_unused_coupon_shop_number = len(unused_coupons) - unused_coupon_shop_number

        max_unused_coupon_shop = np.max(unused_coupons)
        max_buy_without_coupon_shop = np.max(buy_without_coupons)
        max_use_coupon_shop = np.max(use_coupons)
        max_total_coupon_shop = np.max(total_coupons)
        max_total_buy_shop = np.max(total_buy)

        avg_unused_coupon_shop = np.mean(unused_coupons)
        avg_buy_without_coupon_shop = np.mean(buy_without_coupons)
        avg_use_coupon_shop = np.mean(use_coupons)
        avg_total_coupon_shop = np.mean(total_coupons)
        avg_total_buy_shop = np.mean(total_buys)

        if unused_coupon > 0:
            avg_unused_coupon_shop_nonzero = np.mean(unused_coupons[np.nonzero(unused_coupons)])
        else:
            avg_unused_coupon_shop_nonzero = 0
        if buy_without_coupon > 0:
            avg_buy_without_coupon_shop_nonzero = np.mean(buy_without_coupons[np.nonzero(buy_without_coupons)])
        else:
            avg_buy_without_coupon_shop_nonzero = 0
        if use_coupon > 0:
            avg_use_coupon_shop_nonzero = np.mean(use_coupons[np.nonzero(use_coupons)])
        else:
            avg_use_coupon_shop_nonzero = 0
        if total_coupon > 0:
            avg_total_coupon_shop_nonzero = np.mean(total_coupons[np.nonzero(total_coupons)])
        else:
            avg_total_coupon_shop_nonzero = 0
        if total_buy > 0:
            avg_total_buy_shop_nonzero = np.mean(total_buys[np.nonzero(total_buys)])
        else:
            avg_total_buy_shop_nonzero = 0

        var_unused_coupon_shop = np.var(unused_coupons)
        var_buy_without_coupon_shop = np.var(buy_without_coupons)
        var_use_coupon_shop = np.var(use_coupons)
        var_total_coupon_shop = np.var(total_coupons)
        var_total_buy_shop = np.var(total_buys)

        if unused_coupon > 0:
            var_unused_coupon_shop_nonzero = np.var(unused_coupons[np.nonzero(unused_coupons)])
        else:
            var_unused_coupon_shop_nonzero = 0
        if buy_without_coupon > 0:
            var_buy_without_coupon_shop_nonzero = np.var(buy_without_coupons[np.nonzero(buy_without_coupons)])
        else:
            var_buy_without_coupon_shop_nonzero = 0
        if use_coupon > 0:
            var_use_coupon_shop_nonzero = np.var(use_coupons[np.nonzero(use_coupons)])
        else:
            var_use_coupon_shop_nonzero = 0
        if total_coupon > 0:
            var_total_coupon_shop_nonzero = np.var(total_coupons[np.nonzero(total_coupons)])
        else:
            var_total_coupon_shop_nonzero = 0
        if total_buy > 0:
            var_total_buy_shop_nonzero = np.var(total_buys[np.nonzero(total_buys)])
        else:
            var_total_buy_shop_nonzero = 0

        if unused_coupon > 0:
            mid_unused_coupon_shop_nonzero = np.median(unused_coupons[np.nonzero(unused_coupons)])
        else:
            mid_unused_coupon_shop_nonzero = 0
        if buy_without_coupon > 0:
            mid_buy_without_coupon_shop_nonzero = np.median(buy_without_coupons[np.nonzero(buy_without_coupons)])
        else:
            mid_buy_without_coupon_shop_nonzero = 0
        if use_coupon > 0:
            mid_use_coupon_shop_nonzero = np.median(use_coupons[np.nonzero(use_coupons)])
        else:
            mid_use_coupon_shop_nonzero = 0
        if total_coupon > 0:
            mid_total_coupon_shop_nonzero = np.median(total_coupons[np.nonzero(total_coupons)])
        else:
            mid_total_coupon_shop_nonzero = 0
        if total_buy > 0:
            mid_total_buy_shop_nonzero = np.median(total_buys[np.nonzero(total_buys)])
        else:
            mid_total_buy_shop_nonzero = 0

        if unused_coupon > 0:
            min_unused_coupon_shop_nonzero = np.min(unused_coupons[np.nonzero(unused_coupons)])
        else:
            min_unused_coupon_shop_nonzero = 0
        if buy_without_coupon > 0:
            min_buy_without_coupon_shop_nonzero = np.min(buy_without_coupons[np.nonzero(buy_without_coupons)])
        else:
            min_buy_without_coupon_shop_nonzero = 0
        if use_coupon > 0:
            min_use_coupon_shop_nonzero = np.min(use_coupons[np.nonzero(use_coupons)])
        else:
            min_use_coupon_shop_nonzero = 0
        if total_coupon > 0:
            min_total_coupon_shop_nonzero = np.min(total_coupons[np.nonzero(total_coupons)])
        else:
            min_total_coupon_shop_nonzero = 0
        if total_buy > 0:
            min_total_buy_shop_nonzero = np.min(total_buys[np.nonzero(total_buys)])
        else:
            min_total_buy_shop_nonzero = 0

        user_table.set_row(row_index, np.array([uid, \
            unused_coupon, buy_without_coupon, use_coupon, total_coupon, total_buy, \
            act_ratio_0, act_ratio_1, act_ratio_2, \
            used_ratio, unused_ratio, buy_with_coupon_ratio, buy_without_coupon_ratio, \
            unused_coupon_shop_number, buy_without_coupon_shop_number, use_coupon_shop_number, pure_unused_coupon_shop_number, pure_no_unused_coupon_shop_number, \
            max_unused_coupon_shop, max_buy_without_coupon_shop, max_use_coupon_shop, max_total_coupon_shop, max_total_buy_shop, \
            avg_unused_coupon_shop, avg_buy_without_coupon_shop, avg_use_coupon_shop, avg_total_coupon_shop, avg_total_buy_shop, \
            avg_unused_coupon_shop_nonzero, avg_buy_without_coupon_shop_nonzero, avg_use_coupon_shop_nonzero, avg_total_coupon_shop_nonzero, avg_total_buy_shop_nonzero, \
            var_unused_coupon_shop, var_buy_without_coupon_shop, var_use_coupon_shop, var_total_coupon_shop, var_total_buy_shop, \
            var_unused_coupon_shop_nonzero, var_buy_without_coupon_shop_nonzero, var_use_coupon_shop_nonzero, var_total_coupon_shop_nonzero, var_total_buy_shop_nonzero, \
            mid_unused_coupon_shop_nonzero, mid_buy_without_coupon_shop_nonzero, mid_use_coupon_shop_nonzero, mid_total_coupon_shop_nonzero, mid_total_buy_shop_nonzero, \
            min_unused_coupon_shop_nonzero, min_buy_without_coupon_shop_nonzero, min_use_coupon_shop_nonzero, min_total_coupon_shop_nonzero, min_total_buy_shop_nonzero]))
        row_index += 1
    assert row_index == len(all_users)
    user_table.check_point("user_act_counts_month_{0}".format(month))

if __name__ == '__main__':
    for i in xrange(1, 7):
        aggregate_shops_by_month(i)
