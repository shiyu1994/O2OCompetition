import numpy as np

path = '/Users/shiyu/Projects/o2o_dis/ccf_data_revised/'
path_features = '/Users/shiyu/Projects/o2o_multi_stage/my_data/'

def gen():
    test_keys = np.genfromtxt(path + 'ccf_offline_stage1_test_revised.csv', delimiter=',', dtype=str)
    probs = np.genfromtxt(path_features + 'result.csv', delimiter=',', dtype=float)

    n = len(test_keys)
    result = np.zeros((n, 4), dtype=float)
    for i in xrange(n):
        uid, mid, cid, dis_rate, dist, date_rec = test_keys[i]
        result[i] = np.array([int(uid), int(cid), int(date_rec), probs[i]])

    np.savetxt(path_features + 'submit_11_2_24.csv', result, delimiter=',', fmt="%d,%d,%d,%10.20f")

if __name__ == '__main__':
    gen()
