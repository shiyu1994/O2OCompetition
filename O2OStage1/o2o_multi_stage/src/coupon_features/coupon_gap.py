import numpy as np
from src.utils.matrix import Matrix
from src.utils.hashset import HashSet
import src.metainfo.paths as paths
from src.feature_operations import append_feature_by_month

def parse_date(date_str):
    date_int = int(date_str)
    year = date_int / 10000
    month = date_int / 100 % 100
    date = date_int % 100
    return np.datetime64('%d-%02d-%02d' % (year, month, date))  

def days_dis(date1, date2, decay_days=15):
    return (parse_date(date2) - parse_date(date1)) / np.timedelta64(1, 'D')

def gen_coupon_gap(X, month):
    get_coupon_history = HashSet()
    get_coupon_history_user = HashSet()
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        date_rec_str = X.get_cell(i, "date_rec")
        if date_rec_str != 'null':
            history_list = get_coupon_history.get(uid_str, HashSet()).get(mid_str, [])
            #if date_rec_str not in history_list:
            history_list.append(date_rec_str)
            user_history_list = get_coupon_history_user.get(uid_str, [])
            #if date_rec_str not in user_history_list:
            user_history_list.append(date_rec_str)
    if month > 1:
        last_X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_1_month{0}.csv'.format(month - 1), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
        for i in xrange(last_X.ndata):
            if i % 100000 == 0:
                print i
            uid_str = last_X.get_cell(i, "uid")
            mid_str = last_X.get_cell(i, "mid")
            date_rec_str = last_X.get_cell(i, "date_rec")
            if date_rec_str != 'null':
                history_list = get_coupon_history.get(uid_str, HashSet()).get(mid_str, [])
                #if date_rec_str not in history_list:
                history_list.append(date_rec_str)
                user_history_list = get_coupon_history_user.get(uid_str, [])
                #if date_rec_str not in user_history_list:
                user_history_list.append(date_rec_str)
    if month < 7:
        next_X = Matrix(np.genfromtxt(paths.ccf_path + 'offline_1_month{0}.csv'.format(month + 1), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
        for i in xrange(next_X.ndata):
            if i % 100000 == 0:
                print i
            uid_str = next_X.get_cell(i, "uid")
            mid_str = next_X.get_cell(i, "mid")
            date_rec_str = next_X.get_cell(i, "date_rec")
            if date_rec_str != 'null':
                history_list = get_coupon_history.get(uid_str, HashSet()).get(mid_str, [])
                #if date_rec_str not in history_list:
                history_list.append(date_rec_str)
                user_history_list = get_coupon_history_user.get(uid_str, [])
                #if date_rec_str not in user_history_list:
                user_history_list.append(date_rec_str)
    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        history_list = get_coupon_history.get(uid_str, HashSet()).get(mid_str, [])
        get_coupon_history.get(uid_str).set(mid_str, np.sort(history_list))
        user_history_list = get_coupon_history_user.get(uid_str, [])
        get_coupon_history_user.set(uid_str, np.sort(user_history_list))
    col_names = ["prev_gap", "next_gap", "user_prev_gap", "user_next_gap", "in_between", "prev_gap_prev", "next_gap_prev"]
    gaps = np.zeros((X.ndata, len(col_names)))

    for i in xrange(X.ndata):
        if i % 100000 == 0:
            print i
        uid_str = X.get_cell(i, "uid")
        mid_str = X.get_cell(i, "mid")
        date_rec_str = X.get_cell(i, "date_rec")
        history_list = get_coupon_history.get(uid_str).get(mid_str)
        user_history_list = get_coupon_history_user.get(uid_str)
        k = -1
        for j in xrange(len(history_list)):
            ##if history_list[j] == date_rec_str:
            if j == len(history_list) - 1 or history_list[j + 1] > date_rec_str:
                break
            else:
                k += 1
        in_between = 1
        if k > 0:
            prev_gap = days_dis(history_list[k], date_rec_str)
        else:
            prev_gap = 0
            in_between = 0
        if k + 2 < len(history_list):
            next_gap = days_dis(date_rec_str, history_list[k + 2])
        else:
            next_gap = 0
            in_between = 0

        k = -1
        for j in xrange(len(user_history_list)):
            ##if user_history_list[j] == date_rec_str:
            if j == len(history_list) - 1 or history_list[j + 1] > date_rec_str:
                break
            else:
                k += 1
        if k > 0:
            user_prev_gap = days_dis(user_history_list[k], date_rec_str)
        else:
            user_prev_gap = 0
        if k + 2 < len(user_history_list):
            user_next_gap = days_dis(date_rec_str, user_history_list[k + 2])
        else:
            user_next_gap = 0

        k = -1
        for j in xrange(len(history_list)):
            if history_list[j] == date_rec_str:
            ##if j == len(history_list) - 1 or history_list[j + 1] > date_rec_str:
                break
            else:
                k += 1
        if k > 0:
            prev_gap_prev = days_dis(history_list[k], date_rec_str)
        else:
            prev_gap_prev = 0
        if k + 2 < len(history_list):
            next_gap_prev = days_dis(date_rec_str, history_list[k + 2])
        else:
            next_gap_prev = 0
            in_between = 0


        gaps[i, :] = np.array([prev_gap, next_gap, user_prev_gap, user_next_gap, in_between, prev_gap_prev, next_gap_prev])
    X.cat_col(gaps, col_names, ["%s" for i in xrange(len(col_names))])
    X.check_point("X_{0}_{1}{2}_gap".format(month, 1, month - 1))

if __name__ == '__main__':
    for i in xrange(2, 8):
        print i
        append_feature_by_month(gen_coupon_gap, i)
