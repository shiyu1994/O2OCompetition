from src.utils.matrix import Matrix
import numpy as np
import src.metainfo.paths as paths
from src.feature_operations import append_feature_by_month
from src.utils.hashset import HashSet

def gen_user_shop_features(path_to_user_features_csv, path_to_shop_features_csv, matrix):
    ufeatures = HashSet(np.genfromtxt(path_to_user_features_csv, delimiter=',', dtype=float))
    sfeatures = HashSet(np.genfromtxt(path_to_shop_features_csv, delimiter=',', dtype=float))
    matrix.join_op("uid", "mid", "score2", ufeatures, sfeatures, lambda x, y: np.sum(np.array(x) * np.array(y)), "%s")
    matrix.join("uid", ["user_cofi_features_auto{0}".format(i) for i in xrange(ufeatures.row_dim)], ufeatures, ["%s" for i in xrange(ufeatures.row_dim)])
    matrix.join("mid", ["shop_cofi_features_auto{0}".format(i) for i in xrange(sfeatures.row_dim)], sfeatures, ["%s" for i in xrange(sfeatures.row_dim)])

def append_score(X, month):
    gen_user_shop_features(paths.my_path + 'u_features_auta_10_{0}_{1}{2}.csv'.format(month, 1, month - 1), \
        paths.my_path + 'i_features_auta_10_{0}_{1}{2}.csv'.format(month, 1, month - 1), X)
    X.check_point("X_{0}_{1}{2}_new_score".format(month, 1, month - 1)) 

if __name__ == '__main__':
    for month in xrange(2, 7):
        append_feature_by_month(append_score, month)
