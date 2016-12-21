import numpy as np
import xgboost
from code.utils.matrix import Matrix
from code.metainfo import paths
from code.utils.auc import *

def gen_data():
    train_sets = {}
    ndata = 0
    for i in xrange(5):
        train_sets[i + 2] = gen_by_month(i + 2)
        ndata += train_sets[i + 2].ndata
    test_set_str = gen_by_month(7)

    nfeature = len(train_sets[2].col_names) - 8

    big_train_set = np.zeros((ndata, nfeature + 1), dtype=float)
    start = 0
    for i in xrange(5):
        big_train_set[start : start + train_sets[i + 2].ndata, :] = train_sets[i + 2].matrix[:, 7:]
        start += train_sets[i + 2].ndata

    test_set = np.zeros((test_set_str.shape[0], nfeature), dtype=float)
    test_set[:, :] = test_set_str[:, 6:]

    for i in xrange(5):
        np.savetxt(paths.my_path + 'train_set{0}.csv'.format(i + 2), train_sets[i + 2][:, 7:], fmt=",".join("%s" for i in xrange(nfeature + 1)))
    np.savetxt(paths.my_path + 'big_train_set.csv', big_train_set, fmt=",".join("%10.20f" for i in xrange(nfeature + 1)))
    np.savetxt(paths.my_path + 'test_set.csv', test_set, fmt=",".join("%10.20f" for i in xrange(nfeature)))

def recover_from_gen_data():
    train_sets = {}
    ndata = 0
    for i in xrange(5):
        train_sets[i + 2] = np.genfromtxt(paths.my_path + 'X{0}.csv'.format(i + 2), delimiter=',', dtype=str)
        ndata += train_sets[i + 2].shape[0]
    test_set_str = np.genfromtxt(paths.my_path + 'X7.csv', delimiter=',', dtype=str)
    nfeatures = train_sets[2].shape[1] - 8

    big_train_set = np.zeros((ndata, nfeatures + 1), dtype=float)
    train_set_float = {}
    start = 0
    for i in xrange(5):
        big_train_set[start : start + train_sets[i + 2].shape[0], :] = train_sets[i + 2][:, 7:]

    test_set = np.zeros((test_set_str.shape[0], nfeatures), dtype=float)
    test_set[:, :] = test_set_str[:, 6:]

    for i in xrange(5):
        np.savetxt(paths.my_path + 'train_set{0}.csv'.format(i + 2), train_sets[i + 2][:, 7:], fmt=",".join("%s" for i in xrange(nfeatures + 1)))
    np.savetxt(paths.my_path + 'big_train_set.csv', big_train_set, fmt=",".join("%10.20f" for i in xrange(nfeatures + 1)))
    np.savetxt(paths.my_path + 'test_set.csv', test_set, fmt=",".join("%10.20f" for i in xrange(nfeatures)))

def train():
    big_train_set = np.genfromtxt(paths.my_path + 'big_train_set.csv', delimiter=',', dtype=float)
    test_set = np.genfromtxt(paths.my_path + 'test_set.csv', delimiter=',', dtype=float)
    dtrain = xgboost.DMatrix(big_train_set[:, :-1], big_train_set[:, -1], missing=-9999.0)
    dtest = xgboost.DMatrix(test_set[:, :], missing=-9999.0)
    params = {'max_depth':6, 'eta':0.1, 'silent':0, 'objective':'binary:logistic', 'bst:subsample':0.8, 'bst:colsample_bytree':0.8}
    params['eval_metric'] = 'auc'
    num_round = 30

    bst = xgboost.train(params, dtrain, num_round)

    probs_test = bst.predict(dtest)
    np.savetxt(paths.my_path + 'result.csv', probs_test, fmt="%10.20f")

def train_with_features(selected_features_file, num_round, cv=False):
    selected_features = np.genfromtxt(paths.my_path + selected_features_file, delimiter=',', dtype=str)
    X_train = np.zeros((0, len(selected_features)), dtype=float)
    y_train = np.zeros(0, dtype=int)

    if cv:
        last_month = 6
    else:
        last_month = 7
    for i in xrange(3, last_month):
        print "loading", i
        col_names = np.genfromtxt(paths.my_path + 'X{0}_col_names_train.csv'.format(i), delimiter=',', dtype=str)
        name_index = {}
        for j in xrange(col_names.shape[1]):
            name_index[col_names[0, j]] = int(col_names[1, j])
        selected_feature_indices = []
        for name in selected_features:
            selected_feature_indices.append(name_index[name])

        matrix = np.genfromtxt(paths.my_path + 'X{0}.csv'.format(i), delimiter=',', dtype=str)
        dmatrix = np.zeros((matrix.shape[0], len(selected_features)), dtype=float)
        dmatrix[:, :] = matrix[:, selected_feature_indices]
        dy = np.zeros(matrix.shape[0], dtype=float)
        dy[:] = matrix[:, name_index["y"]]
        X_train = np.vstack((X_train, dmatrix))
        y_train = np.hstack((y_train, dy))

    col_names = np.genfromtxt(paths.my_path + 'X{0}_col_names_train.csv'.format(last_month), delimiter=',', dtype=str)
    name_index = {}
    for j in xrange(col_names.shape[1]):
        name_index[col_names[0, j]] = int(col_names[1, j])
    selected_feature_indices = []
    for name in selected_features:
        selected_feature_indices.append(name_index[name])
    matrix = np.genfromtxt(paths.my_path + 'X{0}.csv'.format(last_month), delimiter=',', dtype=str)
    X_test = np.zeros((matrix.shape[0], len(selected_features)), dtype=float)
    X_test[:, :] = matrix[:, selected_feature_indices]

    if cv:
        y_test = np.zeros(matrix.shape[0], dtype=int)
        y_test[:] = matrix[:, name_index["y"]]
        cids_test = np.zeros(matrix.shape[0], dtype=int)
        cids_test[:] = matrix[:, name_index["f2"]]

    X_train_matrix = xgboost.DMatrix(X_train, y_train)

    if cv:
        X_test_matrix = xgboost.DMatrix(X_test, y_test)
    else:
        X_test_matrix = xgboost.DMatrix(X_test)

    params = {'max_depth':5, 'eta':0.05, 'silent':0, 'objective':'binary:logistic', 'bst:subsample':0.8, 'bst:colsample_bytree':0.8}
    params['eval_metric'] = 'auc'

    if cv:
        bst = xgboost.train(params, X_train_matrix, num_round, evals=[(X_test_matrix, 'X_test')], feval=lambda _probs, _dtrain: auc_eval(_probs, _dtrain, cids_test))
    else:
        bst = xgboost.train(params, X_train_matrix, num_round)

    probs_test = bst.predict(X_test_matrix)
    if cv:
        np.savetxt(paths.my_path + 'result_cv.csv', probs_test, fmt="%10.20f")
        print "avg auc"
        avg_score = auc_score_avg(probs_test, y_test, cids_test)
        print avg_score

        fscores = bst.get_fscore()
        #bst.dump_model("model.txt")
        #bst.save_model("model_from_Jun.model")

        eval_record = open('eval_record_cv.txt', 'w')
    else:
        np.savetxt(paths.my_path + 'result.csv', probs_test, fmt="%10.20f")
        eval_record = open('eval_record.txt', 'w')

    fscores = bst.get_fscore()
    #bst.dump_model("model.txt")
    #bst.save_model("model_from_Jun.model")
    for key in fscores:
        eval_record.write(key + ":" + str(fscores[key]) + "\n")
    if cv:
        eval_record.write(str(avg_score))
    eval_record.write("\n")

if __name__ == '__main__':
    #gen_data()
    #recover_from_gen_data()
    #cv()
    #train()
    train_with_features("features.csv", 1000, cv=True)
    pass
