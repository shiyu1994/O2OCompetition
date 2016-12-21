import numpy as np
from code.utils.hashset import HashSet
from code.utils.matrix import Matrix
import code.metainfo.paths as paths

def calc_give_after_purchase():
    offline = Matrix(np.genfromtxt(paths.ccf_path + 'ccf_offline_stage1_train.csv', delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    #online = Matrix(np.genfromtxt(paths.ccf_path + 'ccf_online_stage1_train.csv', delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])
    user_shop_coupon_buy = HashSet()
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        if date_str != 'null':
            date_list = user_shop_coupon_buy.get(uid_str, HashSet()).get(mid_str, [])
            date_list.append(date_str)

    shop_coupon = {}
    shop_give_after_purchase = {}
    shop_give_after_purchase_used = {}
    shop_coupon_used = {}

    #user_shop_coupon = HashSet()
    #user_shop_coupon_give_after_purchase = HashSet()
    #user_shop_coupon_give_after_purchase_used = HashSet()
    #user_shop_coupon_used = HashSet()

    #user_coupon = HashSet()
    #user_give_after_purchase = HashSet()
    #user_give_after_purchase_used = HashSet()
    #user_coupon_used = HashSet()

    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        date_str = offline.get_cell(i, "date")
        cid_str = offline.get_cell(i, "cid")
        date_rec_str = offline.get_cell(i, "date_rec")
        date_list = user_shop_coupon_buy.get(uid_str, HashSet()).get(mid_str, [])
        if cid_str != 'null':
            #user_shop_coupon.get(uid_str, HashSet()).add_one(mid_str)
            #user_coupon.add_one(uid_str)
            if mid_str not in shop_coupon:
                shop_coupon[mid_str] = 1
            else:
                shop_coupon[mid_str] += 1
            date_list = user_shop_coupon_buy.get(uid_str, HashSet()).get(mid_str, [])
            if date_rec_str in date_list:
                #user_shop_coupon_give_after_purchase.get(uid_str, HashSet()).add_one(mid_str)
                #user_give_after_purchase.add_one(uid_str)
                if mid_str not in shop_give_after_purchase:
                    shop_give_after_purchase[mid_str] = 1
                else:
                    shop_give_after_purchase[mid_str] += 1
                if date_str != 'null':
                    #user_shop_coupon_give_after_purchase_used.get(uid_str, HashSet()).add_one(mid_str)
                    #user_give_after_purchase_used.add_one(uid_str)
                    if mid_str not in shop_give_after_purchase_used:
                        shop_give_after_purchase_used[mid_str] = 1
                    else:
                        shop_give_after_purchase_used[mid_str] += 1
            if date_str != 'null':
                #user_shop_coupon_used.get(uid_str, HashSet()).add_one(mid_str)
                #user_coupon_used.add_one(uid_str)
                if mid_str not in shop_coupon_used:
                    shop_coupon_used[mid_str] = 1
                else:
                    shop_coupon_used[mid_str] += 1

    give_after_purchase_statistics_names = ["shop_coupon", "shop_coupon_used", "shop_coupon_give_after_purchase", "shop_coupon_give_after_purchase_used"]

    give_after_purchase_statistics = np.zeros((offline.ndata, len(give_after_purchase_statistics_names)))
    for i in xrange(offline.ndata):
        if i % 100000 == 0:
            print i
        uid_str = offline.get_cell(i, "uid")
        mid_str = offline.get_cell(i, "mid")
        shop_coupon_counter = shop_coupon.get(mid_str, 0)
        shop_coupon_used_counter = shop_coupon_used.get(mid_str, 0)
        shop_coupon_give_after_purchase_counter = shop_give_after_purchase.get(mid_str, 0)
        shop_coupon_give_after_purchase_used_counter = shop_give_after_purchase_used.get(mid_str, 0)
        #if uid_str in user_shop_coupon:
        #    user_shop_coupon_counter = user_shop_coupon.get(uid_str).get(mid_str, 0)
        #else:
        #    user_shop_coupon_counter = 0
        #if uid_str in user_shop_coupon_used:
        #    user_shop_coupon_used_counter = user_shop_coupon_used.get(uid_str).get(mid_str, 0)
        #else:
        #    user_shop_coupon_used_counter = 0
        #if uid_str in user_shop_coupon_give_after_purchase:
        #    user_shop_coupon_give_after_purchase_counter = user_shop_coupon_give_after_purchase.get(mid_str, 0)
        #else:
        #    user_shop_coupon_give_after_purchase_counter = 0
        #if uid_str in user_shop_coupon_give_after_purchase_used:
        #    user_shop_coupon_give_after_purchase_used_counter = user_shop_coupon_give_after_purchase_used.get(mid_str, 0)
        #else:
        #    user_shop_coupon_give_after_purchase_used_counter = 0

        #if not user_coupon_used.has(uid_str) or user_coupon.get(uid_str) == 0:
        #    user_coupon_use_ratio = 0
        #else:
        #    user_coupon_use_ratio = user_coupon_used.get(uid_str) * 1.0 / user_coupon.get(uid_str)

        #if not user_give_after_purchase_used.has(uid_str) or user_give_after_purchase_used.get(uid_str) == 0:
        #    user_give_after_purchase_use_ratio = 0
        #else:
        #    user_give_after_purchase_use_ratio = user_give_after_purchase_used.get(uid_str) * 1.0 / user_give_after_purchase.get(uid_str)

        #if not shop_give_after_purchase_used.has(mid_str) or shop_give_after_purchase_used.get(mid_str) == 0:
        #    shop_coupon_give_after_purchase_use_ratio = 0
        #else:
        #    shop_coupon_give_after_purchase_use_ratio = shop_give_after_purchase_used.get(mid_str) * 1.0 / shop_give_after_purchase.get(mid_str)

        #if not user_shop_coupon_used.has(uid_str) or not user_shop_coupon_used.get(uid_str).has(mid_str):
        #    user_shop_coupon_use_ratio = 0
        #else:
        #    user_shop_coupon_use_ratio = user_shop_coupon_used.get(uid_str).get(mid_str) * 1.0 / user_shop_coupon.get(uid_str).get(mid_str)

        #if not user_shop_coupon_give_after_purchase_used.has(uid_str) or not user_shop_coupon_give_after_purchase.get(uid_str).has(mid_str):
        #    user_shop_coupon_use_after_purchase_ratio = 0
        #else:
        #    user_shop_coupon_use_after_purchase_ratio = user_shop_coupon_give_after_purchase_used.get(uid_str).get(mid_str) * 1.0 / user_shop_coupon_give_after_purchase.get(uid_str).get(mid_str)

        give_after_purchase_statistics[i, :] = np.array([shop_coupon_counter, shop_coupon_used_counter, shop_coupon_give_after_purchase_counter, shop_coupon_give_after_purchase_used_counter])
    offline.cat_col(give_after_purchase_statistics, give_after_purchase_statistics_names, ["%s" for i in xrange(len(give_after_purchase_statistics_names))])
    offline.check_point("give_after_purchase_statistics")

if __name__ == '__main__':
    calc_give_after_purchase()
