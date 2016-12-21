import numpy as np
from code.utils.matrix import Matrix
from code.utils.hashset import HashSet
import code.metainfo.paths as paths

def calc_score_from_history(user_shop_history):
    state = 0
    score = 0
    #print "user_shop_history", user_shop_history
    for date, act in user_shop_history:
        if act == 2:
            if state == 0:
                score -= 2
                state = 0
            elif state == 1:
                score -= 1
                state = 3
            elif state == 2:
                score -= 1
                state = 3
            elif state == 3:
                score -= 1
                state = 3
        elif act == 1:
            if state == 0:
                score += 1
                state = 1
            elif state == 1:
                score += 1
                state = 1
            elif state == 2:
                score += 1
                state = 1
            elif state == 3:
                score += 1
                state = 1
        elif act == 0:
            if state == 0:
                score += 2
                state = 2
            elif state == 1:
                score += 2
                state = 2
            elif state == 2:
                score += 2
                state = 2
            elif state == 3:
                score += 2
                state = 2
    #print "score", score
    return score

def calc_score(month):
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'offline_train_{0}.csv'.format(month), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    history = HashSet()
    score = HashSet()
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        date_rec_str = offline.get_cell(i, "date_rec")
        history_list = history.get(uid_str, HashSet()).get(mid_str, [])
        if cid_str != 'null' and date_str == 'null':
            assert date_rec_str != 'null'
            history_list.append((int(date_rec_str), 2))
        elif cid_str == 'null' and date_str != 'null':
            history_list.append((int(date_str), 1))
        else:
            history_list.append((int(date_str), 0))
    for uid in history.get_keys():
        user_history = history.get(uid)
        for mid in user_history.get_keys():
            user_shop_history = user_history.get(mid)
            user_history.set(mid, np.sort(np.array(user_shop_history, dtype='f,f')))
    max_score = 0
    min_score = 100000
    sum_score = 0
    counter_score = 0
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        history_tuple_list = history.get(uid_str).get(mid_str)
        #print "history_tuple_list", history_tuple_list
        user_shop_history = np.array(history_tuple_list, dtype='f,f')
        #print "user_shop_history", user_shop_history
        u_s_score = calc_score_from_history(user_shop_history)
        if u_s_score > max_score:
            max_score = u_s_score
        if u_s_score < min_score:   
            min_score = u_s_score
        sum_score += u_s_score
        counter_score += 1
        score.get(uid_str, HashSet()).set(mid_str, calc_score_from_history(user_shop_history))
    print "max_score", max_score
    print "min_score", min_score
    print "avg_score", sum_score * 1.0 / counter_score
    return score

if __name__ == '__main__':
    a = []
    a.append((2013, 1))
    a.append((2014, 2))
    a.append((2015, 0))
    b = np.array(a, dtype='f,f')
    print np.sort(b)
