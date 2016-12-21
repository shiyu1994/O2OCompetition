import numpy as np
from src.feature_operations import append_feature_by_month

def calc_trend(y):
    n = len(y)
    x = np.arange(n - 1, -1, -1, dtype=float)
    sum_x_2 = np.sum(x ** 2)
    mean_x = np.mean(x)
    sum_x_y = np.sum(x * y)
    mean_y = np.mean(y)
    return (sum_x_y - n * mean_x * mean_y) * 1.0 / (sum_x_2 - n * (mean_x ** 2))            

def gen_trend_func(row):
    user_buy = np.zeros(4)
    user_use_coupon = np.zeros(4)
    user_unuse = np.zeros(4)
    for i in xrange(4):
        user_buy[i] = row['recent_user_buy{0}'.format(i)]
        user_use_coupon[i] = row['recent_user_use{0}'.format(i)]
        user_unuse[i] = row['recent_user_unuse{0}'.format(i)]
    n = 0
    for i in xrange(4):
        if user_buy[i] == -9999:
            break
        else:
            n += 1
    if n == 1:
        return np.array([-9999, -9999, -9999])
    else:
        return np.array([calc_trend(np.array(user_buy[:n], dtype=float)), calc_trend(np.array(user_use_coupon[:n], dtype=float)), calc_trend(np.array(user_unuse[:n], dtype=float))])

def gen_trend(X, month):
    X.gen_features(["user_buy_trend", "user_use_trend", "user_unuse_trend"], gen_trend_func, ["%s", "%s", "%s"])
    X.check_point(month)

if __name__ == '__main__':
    for i in xrange(2, 8):
        append_feature_by_month(gen_trend, i)
