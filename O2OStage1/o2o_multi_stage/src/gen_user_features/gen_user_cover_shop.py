import numpy as np
from src.feature_operations import append_feature_by_month
from src.utils.matrix import Matrix
from src.utils.hashset import HashSet

def gen_user_buy_shop(since, to, month):
    assert to == month - 1
    offline = Matrix(np.genfromtxt('offline_{0}_month_{1}{2}.csv'.format(to - since + 1, since, to), delimiter=',', dtype=str), ["uid", "mid", "cid", "dis_rate", "dist", "date_rec", "date"], ["%s" for i in xrange(7)])
    online = Matrix(np.genfromtxt('offline_{0}_month_{1}{2}.csv'.format(to - since + 1, since, to), delimiter=',', dtype=str), ["uid", "mid", "act", "cid", "dis_rate", "date_rec", "date"], ["%s" for i in xrange(7)])

    col_names = np.genfromtxt(paths.my_path + 'col_names_{0}_{1}{2}.csv'.format(month, since, to), delimiter=',', dtype=str)
    col_number = col_names.shape[1]
    X = Matrix(np.genfromtxt(paths.my_path + 'X_{0}_{1}{2}.csv'.format(month, since, to), delimiter=',', dtype=str), list(col_names[0, :]), ["%s" for i in xrange(col_names.shape[1])])

    user_buy_shop = HashSet()
    for i in xrange(offline.ndata):
        
