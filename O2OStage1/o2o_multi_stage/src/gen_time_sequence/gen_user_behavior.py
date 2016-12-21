import numpy as np
from src.feature_operations import append_feature_by_month
from src.utils.matrix import Matrix
from src.metainfo import paths
from src.utils.hashset import HashSet
from src.feature_operations import drop
from src.feature_operations import drop_multiple

def get_month(date_str):
    date_int = int(date_str)
    year = date_int / 10000
    month = date_int / 100 % 100
    date = date_int % 100
    return int((np.datetime64('%d-%02d' % (year, month)) - np.datetime64('2016-01')) / np.timedelta64(1, 'M') + 1)

def gen_user_recent_behavior(X, month):
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt(paths.ccf_path + 'online_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    behaviors = {1:HashSet(), 2:HashSet(), 3:HashSet(), 4:HashSet(), 5:HashSet(), 6:HashSet()}
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        date_rec_str = offline.get_cell(i, "date_rec")  
        if date_str != 'null':
            act_month = get_month(date_str)
            if not behaviors[act_month].has(uid_str):
                behaviors[act_month].set(uid_str, np.zeros(3))
            if cid_str != 'null':
                behaviors[act_month].get(uid_str)[2] += 1
            else:
                behaviors[act_month].get(uid_str)[1] += 1
        else:
            act_month = get_month(date_rec_str)
            if not behaviors[act_month].has(uid_str):
                behaviors[act_month].set(uid_str, np.zeros(3))
            behaviors[act_month].get(uid_str)[0] += 1

    for i in xrange(online.ndata):
        if i % 100000 == 0:
            print i
        uid_str = online.get_cell(i, "uid")
        date_str = online.get_cell(i, "date")
        cid_str = online.get_cell(i, "cid")
        date_rec_str = online.get_cell(i, "date_rec")
        act_str = online.get_cell(i, "act")
        if act_str == '0':
            continue
        if date_str != 'null':
            assert act_str == '1'
            act_month = get_month(date_str)
            if not behaviors[act_month].has(uid_str):
                behaviors[act_month].set(uid_str, np.zeros(3))
            if cid_str != 'null':
                behaviors[act_month].get(uid_str)[2] += 1
            else:
                behaviors[act_month].get(uid_str)[1] += 1
        else:
            assert act_str == '2'
            act_month = get_month(date_rec_str)
            if not behaviors[act_month].has(uid_str):
                behaviors[act_month].set(uid_str, np.zeros(3))
            behaviors[act_month].get(uid_str)[0] += 1

    def gen_user_recent_behavior_func(row):
        date_rec_str = row['date_rec']
        uid_str = row['uid']
        act_month = get_month(date_rec_str)
        behavior_history = np.zeros(0)
        for i in xrange(4):
            behavior_vector = np.array([-9999, -9999, -9999])
            if date_rec_str != 'null' and act_month - 1 - i > 0 and behaviors[act_month - 1 - i].has(uid_str):
                behavior_vector = behaviors[act_month - 1 - i].get(uid_str)
            elif act_month - 1 - i > 0 and date_rec_str != 'null':
                behavior_vector = np.array([0, 0, 0])
            behavior_history = np.hstack((behavior_history, behavior_vector))
        return behavior_history
    names = []
    for i in xrange(4):
        names += ["recent_user_unuse{0}".format(i), "recent_user_buy{0}".format(i), "recent_user_use{0}".format(i)]
    X.gen_features(names, gen_user_recent_behavior_func, ["%s" for i in xrange(12)])
    X.check_point(month)


if __name__ == '__main__':
    for i in xrange(2, 8):
        append_feature_by_month(gen_user_recent_behavior, i)
