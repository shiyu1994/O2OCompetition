import numpy as np
from filtering import CollaborativeFilter
from calc_score import calc_score

ccf_path = '/Users/shiyu/Projects/o2o_competition/ccf_data_revised/'
my_path = '/Users/shiyu/Projects/o2o_competition/my_data/'

def gen_score(month):
    user_shop_score = calc_score(month)
    uid_hash = {}
    mid_hash = {}

    n_user = 0
    n_shop = 0
    pair = 0

    for uid in user_shop_score.get_keys():
        uid_hash[uid] = n_user
        n_user += 1
        user_shop_score_list = user_shop_score.get(uid) 
        for mid in user_shop_score_list.get_keys():
            mid_hash[mid] = n_shop
            n_shop += 1
            pair += 1

    scores = np.zeros((pair, 3), dtype=int)
    cnt = 0
    uid_invert = np.zeros(n_user)
    mid_invert = np.zeros(n_shop)

    for uid in user_shop_score.get_keys():
        user_shop_score_list = user_shop_score.get(uid)
        for mid in user_shop_score_list.get_keys():
            scores[cnt] = np.array([uid_hash[uid], mid_hash[mid], user_shop_score_list.get(mid)])
            uid_invert[uid_hash[uid]] = uid
            mid_invert[mid_hash[mid]] = mid
            cnt += 1

    cfilter = CollaborativeFilter(scores, n_user, n_shop, lr=30, m=10, num_iter=1200, reg=0.5)
    cfilter.train()

    u_features = np.hstack((np.array([uid_invert]).T, cfilter.x))
    i_features = np.hstack((np.array([mid_invert]).T, cfilter.theta))
    np.savetxt(my_path + 'u_features_auta_10_{0}_{1}{2}.csv'.format(month, 1, month - 1), u_features, fmt="%d," + ",".join("%f" for i in xrange(10)))
    np.savetxt(my_path + 'i_features_auta_10_{0}_{1}{2}.csv'.format(month, 1, month - 1), i_features, fmt="%d," + ",".join("%f" for i in xrange(10)))

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
    for month in xrange(2, 7):
        gen_score(month)
