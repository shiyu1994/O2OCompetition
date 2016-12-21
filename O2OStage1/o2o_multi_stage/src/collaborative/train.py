import numpy as np
from filtering import CollaborativeFilter

ccf_path = '/Users/shiyu/Projects/o2o_multi_stage/ccf_data_revised/'
my_path = '/Users/shiyu/Projects/o2o_multi_stage/my_data/'

def gen_used():
    n = len(train_online)
    for i in xrange(n):
        if i % 10000 == 0:
            print i
        if train_online[i, 2] == '1' and train_online[i, 3] != 'null' and train_online[i, 3] != 'fixed':
            cid_int = int(train_online[i, 3])
            used_coup.add(cid_int)

def gen_score(month, since, to):
    train_offline = np.genfromtxt(ccf_path + 'offline_train_{0}_{1}{2}.csv'.format(month, since, to), delimiter=',', dtype=str)
    #train_online = np.genfromtxt(ccf_path + 'online_trai.csv', delimiter=',', dtype=str)

    user_item_score = {}
    for i in xrange(97):
        user_item_score[i] = {}
    uid_hash = {}
    iid_hash = {}

    used_coup = set()

    n_user = 0
    n_item = 0
    pair = 0
    n_data_offline = len(train_offline)
    for i in xrange(n_data_offline):
        if i % 10000 == 0:
            print i
        uid, mid, cid, dist, dis_rate, data_rec, date = train_offline[i]
        uid_int = int(uid)
        mid_int = int(mid)
        gid = uid_int % 97
        if uid_int not in user_item_score[gid]:
            user_item_score[gid][uid_int] = {}
        if mid_int not in user_item_score[gid][uid_int]:
            user_item_score[gid][uid_int][mid_int] = 0
            pair += 1
        if cid == 'null' and date != 'null':
            user_item_score[gid][uid_int][mid_int] += 1
        elif cid != 'null' and date != 'null':
            user_item_score[gid][uid_int][mid_int] += 2
        #elif cid != 'null' and date == 'null':
        #    user_item_score[gid][uid_int][mid_int] -= 1

        if uid_int not in uid_hash:
            uid_hash[uid_int] = n_user
            n_user += 1
        if mid_int not in iid_hash:
            iid_hash[mid_int] = n_item
            n_item += 1

    scores = np.zeros((pair, 3), dtype=int)
    cnt = 0
    uid_invert = np.zeros(n_user)
    iid_invert = np.zeros(n_item)
    for i in xrange(97):
        for u in user_item_score[i]:
            for m in user_item_score[i][u]:
                scores[cnt] = np.array([uid_hash[u], iid_hash[m], user_item_score[i][u][m]])    
                uid_invert[uid_hash[u]] = u
                iid_invert[iid_hash[m]] = m
                cnt += 1

    #scores[:, -1] = (scores[:, -1] - np.mean(scores[:, -1])) / np.sqrt(np.var(scores[:, -1]))
    cfilter = CollaborativeFilter(scores, n_user, n_item, lr=300, m=10, num_iter=1200, reg=0.5)
    cfilter.train()
    #if cfilter.final_loss < 0.99:
    u_features = np.hstack((np.array([uid_invert]).T, cfilter.x))
    i_features = np.hstack((np.array([iid_invert]).T, cfilter.theta))
    np.savetxt(my_path + 'u_features_no_penalty_10_{0}_{1}{2}.csv'.format(month, since, to), u_features, fmt="%d," + ",".join("%f" for i in xrange(10)))
    np.savetxt(my_path + 'i_features_no_penalty_10_{0}_{1}{2}.csv'.format(month, since, to), i_features, fmt="%d," + ",".join("%f" for i in xrange(10)))

    """
    n_data_online = len(train_online)
    for i in xrange(n_data_online):
        if i % 10000 == 0:
            print i
        uid, mid, act, cid, dis_rate, data_rec, date = train_online[i]
        uid_int = int(uid)
        mid_int = int(mid)
        gid = uid_int % 97
        if uid_int not in user_item_score[gid]:
            user_item_score[gid][uid_int] = {}
        if mid_int not in user_item_score[gid][uid_int]:
            user_item_score[gid][uid_int][mid_int] = 0
            pair += 1

        if act == '0':
            user_item_score[gid][uid_int][mid_int] += 0.2
        elif act == '1' and cid == 'null':
            user_item_score[gid][uid_int][mid_int] += 2
        elif act == '1' and cid != 'null':
            user_item_score[gid][uid_int][mid_int] += 1
        elif act == '2' and int(cid) not in used_coup:
            user_item_score[gid][uid_int][mid_int] -= 1

        if uid_int not in uid_hash:
            uid_hash[uid_int] = n_user
            n_user += 1
        if mid_int not in iid_hash:
            iid_hash[mid_int] = n_item
            n_item += 1
    """



if __name__ == '__main__':
    for month in xrange(3, 8):
        for k in xrange(2, month):
            gen_score(month, k, month - 1)
