import numpy as np
import code.metainfo.paths as paths
from code.utils.matrix import Matrix
from code.utils.hashset import HashSet

def divide(x, y):
    if y == 0:
        return 0
    else:
        return x * 1.0 / y

def divide_array(x, y):
    zero_indices = np.nonzero(y == 0)
    x[zero_indices] = 0
    y[zero_indices] = 1

    return x * 1.0 / y

def calc_trend(y):
    n = len(y)
    if n == 0:
        return 0
    x = np.arange(n - 1, -1, -1, dtype=float)
    sum_x_2 = np.sum(x ** 2)
    mean_x = np.mean(x)
    sum_x_y = np.sum(x * y)
    mean_y = np.mean(y)
    return (sum_x_y - n * mean_x * mean_y) * 1.0 / (sum_x_2 - n * (mean_x ** 2))

def calc_trend_matrix(Y):
    n = Y.shape[0]
    if n == 0:
        return np.zeros(Y.shape[1])
    x = np.arange(n - 1, -1, -1, dtype=float)
    sum_x_2 = np.sum(x ** 2)
    mean_x = np.mean(x)
    sum_x_y = np.sum(x * Y.T, 1)
    mean_y = np.mean(Y, 0)
    return (sum_x_y - n * mean_x * mean_y) * 1.0 / (sum_x_2 - n * (mean_x ** 2))

def max_avg_var(Y):
    n = Y.shape[0]
    if n == 0:
        return np.zeros(Y.shape[1])
    return np.hstack((np.max(Y, 0), np.mean(Y, 0), np.var(Y, 0)))

def calc_trend_and_max_avg_var(acts):
    total_coupons = acts[:, 0] + acts[:, 2]
    total_buys = acts[:, 1] + acts[:, 2]
    total_acts = np.array([acts[:, 0] + acts[:, 1] + acts[:, 2]]).T

    used_ratio = np.array([divide_array(acts[:, 2], total_coupons)]).T
    unused_ratio = np.array([divide_array(acts[:, 0], total_coupons)]).T
    buy_with_coupon_ratio = np.array([divide_array(acts[:, 2], total_buys)]).T
    buy_without_coupon_ratio = np.array([divide_array(acts[:, 1], total_buys)]).T

    acts = np.hstack((acts, np.array([total_coupons]).T, np.array([total_buys]).T, divide_array(acts, total_acts), \
        used_ratio, unused_ratio, buy_with_coupon_ratio, buy_without_coupon_ratio))
    trends = calc_trend_matrix(acts)
    return np.hstack((trends, max_avg_var(acts[:, 0:5])))

def aggregate_shops_months(since, to):
    user_act_counts = HashSet()
    for k in xrange(since, to + 1):
        col_names = np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}_col_names.csv'.format(k), delimiter=',', dtype=str)
        user_table = Matrix(np.genfromtxt(paths.my_path + 'user_act_counts_month_{0}.csv'.format(k), delimiter=',', dtype=float), col_names[0, :], 0.0)
        for i in xrange(user_table.ndata):
            uid = user_table.get_cell(i, "uid")
            acts = user_act_counts.get(uid, np.zeros((to + 1 - since, 3)))
            acts[k - since, 0] += user_table.get_cell(i, "unused_coupon")
            acts[k - since, 1] += user_table.get_cell(i, "buy_without_coupon")
            acts[k - since, 2] += user_table.get_cell(i, "use_coupon")

    user_col_names = ["uid", "unused_coupon", "buy_without_coupon", "use_coupon", "total_coupon", "total_buy", \
        "act_ratio_0", "act_ratio_1", "act_ratio_2", \
        "used_ratio", "unused_ratio", "buy_with_coupon_ratio", "buy_without_coupon_ratio", \
        "trend_unused_coupon", "trend_buy_without_coupon", "trend_use_coupon", "trend_total_coupon", "trend_total_buy", \
        "trend_act_ratio_0", "trend_act_ratio_1", "trend_act_ratio_2", \
        "trend_used_ratio", "trend_unused_ratio", "trend_buy_with_coupon_ratio", "trend_buy_without_coupon_ratio", \
        "max_unused_coupon_month", "max_buy_without_coupon_month", "max_use_coupon_month", "max_total_coupon_month", "max_total_buy_month", \
        "avg_unused_coupon_month", "avg_buy_without_coupon_month", "avg_use_coupon_month", "avg_total_coupon_month", "avg_total_buy_month", \
        "var_unused_coupon_month", "var_buy_without_coupon_month", "var_use_coupon_month", "var_total_coupon_month", "var_total_buy_month"]
    ndata = len(user_act_counts.get_keys())
    user_act_table = Matrix(np.zeros((ndata, len(user_col_names))), user_col_names, ["%s" for i in xrange(len(user_col_names))])
    col_index = 0
    for uid in user_act_counts.get_keys():
        acts = user_act_counts.get(uid)     
        unused_coupon = np.sum(acts[:, 0])
        buy_without_coupon = np.sum(acts[:, 1])
        use_coupon = np.sum(acts[:, 2])

        total_coupon = unused_coupon + use_coupon
        total_buy = buy_without_coupon + use_coupon
        total_acts = unused_coupon + buy_without_coupon + use_coupon

        act_ratio_0 = divide(unused_coupon, total_acts)
        act_ratio_1 = divide(buy_without_coupon, total_acts)
        act_ratio_2 = divide(use_coupon, total_acts)

        used_ratio = divide(use_coupon, total_coupon)
        unused_ratio = divide(unused_coupon, total_coupon)
        buy_with_coupon_ratio = divide(use_coupon, total_buy)
        buy_without_coupon_ratio = divide(buy_without_coupon, total_buy)

        user_act_table.set_row(col_index, np.hstack((np.array([uid, unused_coupon, buy_without_coupon, use_coupon, total_coupon, total_buy, \
            act_ratio_0, act_ratio_1, act_ratio_2, \
            used_ratio, unused_ratio, buy_with_coupon_ratio, buy_without_coupon_ratio]), calc_trend_and_max_avg_var(acts))))

        col_index += 1

    user_act_table.check_point("user_act_counts_month_{0}{1}".format(since, to))

if __name__ == '__main__':
    for i in xrange(2, 7):
        aggregate_shops_months(1, i)
