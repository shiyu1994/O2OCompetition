import numpy as np

my_path = '/Users/shiyu/Projects/o2o_multi_stage/my_data/'
ccf_path = '/Users/shiyu/Projects/o2o_multi_stage/ccf_data_revised/'

def split(data_set_name, fname, gen_test=False):
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    X_len = X.shape[0]
    data_sets_by_month = {2:open(data_set_name + '_2.csv', 'w'), 3:open(data_set_name + '_3.csv', 'w'), 4:open(data_set_name + '_4.csv', 'w'), 5:open(data_set_name + '_5.csv', 'w'), 6:open(data_set_name + '_6.csv', 'w'), 7:open(data_set_name + '_7.csv', 'w')}
    if gen_test:
        test_sets_by_month = {2:open(data_set_name + '_test_2.csv', 'w'), 3:open(data_set_name + '_test_3.csv', 'w'), 4:open(data_set_name + '_test_4.csv', 'w'), 5:open(data_set_name + '_test_5.csv', 'w'), 6:open(data_set_name + '_test_6.csv', 'w')}
    split_points = np.array([20160201, 20160301, 20160401, 20160501, 20160601, 20160701])
    for i in xrange(X_len):
        date = 0
        if X[i, 6] != 'null':
            date = int(X[i, 6])
        else:
            date = int(X[i, 5])
        for j in xrange(6):
            if date < split_points[j]:
                data_sets_by_month[j + 2].write(",".join(X[i, :]) + "\n")
        if gen_test and X[i, 5] != 'null':
            date_rec = int(X[i, 5])
            for j in xrange(5):
                if X[i, 2] != 'null' and date_rec >= split_points[j] and date_rec < split_points[j + 1]:
                    test_sets_by_month[j + 2].write(",".join(X[i, :]) + "\n")
                    break
    for month in data_sets_by_month:
        data_sets_by_month[month].close()
    if gen_test:
        for month in test_sets_by_month:
            test_sets_by_month[month].close()

def split_by_month_user(data_set_name, fname):
    data_sets_by_month = {1:open(data_set_name + '1.csv', 'w'), 2:open(data_set_name + '2.csv', 'w'), 3:open(data_set_name + '3.csv', 'w'), 4:open(data_set_name + '4.csv', 'w'), 5:open(data_set_name + '5.csv', 'w'), 6:open(data_set_name + '6.csv', 'w')}
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    for i in xrange(X.shape[0]):
        date_str = X[i, 6]
        date_rec_str = X[i, 5]
        if date_str != 'null':
            if date_str >= '20160101' and date_str < '20160201':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160201' and date_str < '20160301':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160301' and date_str < '20160401':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160401' and date_str < '20160501':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160501' and date_str < '20160601':
                data_sets_by_month[5].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160601' and date_str < '20160701':
                data_sets_by_month[6].write(",".join(X[i, :]) + "\n")
        elif date_rec_str != 'null':
            if date_rec_str >= '20160101' and date_rec_str < '20160201':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            elif date_rec_str >= '20160201' and date_rec_str < '20160301':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            elif date_rec_str >= '20160301' and date_rec_str < '20160401':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            elif date_rec_str >= '20160401' and date_rec_str < '20160501':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
            elif date_rec_str >= '20160501' and date_rec_str < '20160601':
                data_sets_by_month[5].write(",".join(X[i, :]) + "\n")
            elif date_rec_str >= '20160601' and date_rec_str < '20160701':
                data_sets_by_month[6].write(",".join(X[i, :]) + "\n")
    for i in xrange(1, 7):
        data_sets_by_month[i].close()

def split_by_2_month_user(data_set_name, fname):
    data_sets_by_month = {1:open(data_set_name + '12.csv', 'w'), 2:open(data_set_name + '23.csv', 'w'), 3:open(data_set_name + '34.csv', 'w'), 4:open(data_set_name + '45.csv', 'w'), 5:open(data_set_name + '56.csv', 'w')}
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    for i in xrange(X.shape[0]):
        date_str = X[i, 6]
        date_rec_str = X[i, 5]
        if date_str != 'null':
            if date_str >= '20160101' and date_str < '20160301':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160201' and date_str < '20160401':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160301' and date_str < '20160501':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160401' and date_str < '20160601':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160501' and date_str < '20160701':
                data_sets_by_month[5].write(",".join(X[i, :]) + "\n")
        elif date_rec_str != 'null':
            if date_rec_str >= '20160101' and date_rec_str < '20160301':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160201' and date_rec_str < '20160401':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160301' and date_rec_str < '20160501':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160401' and date_rec_str < '20160601':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160501' and date_rec_str < '20160701':
                data_sets_by_month[5].write(",".join(X[i, :]) + "\n")
    for i in xrange(1, 6):
        data_sets_by_month[i].close()

def split_by_3_month_user(data_set_name, fname):
    data_sets_by_month = {1:open(data_set_name + '13.csv', 'w'), 2:open(data_set_name + '24.csv', 'w'), 3:open(data_set_name + '35.csv', 'w'), 4:open(data_set_name + '46.csv', 'w')}
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    for i in xrange(X.shape[0]):
        date_str = X[i, 6]
        date_rec_str = X[i, 5]
        if date_str != 'null':
            if date_str >= '20160101' and date_str < '20160401':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160201' and date_str < '20160501':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160301' and date_str < '20160601':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160401' and date_str < '20160701':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
        elif date_rec_str != 'null':
            if date_rec_str >= '20160101' and date_rec_str < '20160401':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160201' and date_rec_str < '20160501':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160301' and date_rec_str < '20160601':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160401' and date_rec_str < '20160701':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
    for i in xrange(1, 4):
        data_sets_by_month[i].close()

def split_by_4_month_user(data_set_name, fname):
    data_sets_by_month = {1:open(data_set_name + '14.csv', 'w'), 2:open(data_set_name + '25.csv', 'w'), 3:open(data_set_name + '36.csv', 'w')}
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    for i in xrange(X.shape[0]):
        date_str = X[i, 6]
        date_rec_str = X[i, 5]
        if date_str != 'null':
            if date_str >= '20160101' and date_str < '20160501':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160201' and date_str < '20160601':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_str >= '20160301' and date_str < '20160701':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
        elif date_rec_str != 'null':
            if date_rec_str >= '20160101' and date_rec_str < '20160501':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160201' and date_rec_str < '20160601':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            if date_rec_str >= '20160301' and date_rec_str < '20160701':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
    for i in xrange(1, 3):
        data_sets_by_month[i].close()

def split_by_month_shop(data_set_name, fname):
    data_sets_by_month = {1:open(data_set_name + '1.csv', 'w'), 2:open(data_set_name + '2.csv', 'w'), 3:open(data_set_name + '3.csv', 'w'), 4:open(data_set_name + '4.csv', 'w'), 5:open(data_set_name + '5.csv', 'w'), 6:open(data_set_name + '6.csv', 'w')}
    X = np.genfromtxt(ccf_path + fname, delimiter=',', dtype=str)
    for i in xrange(X.shape[0]):
        date_str = X[i, 6]
        date_rec_str = X[i, 5]
        if date_str != 'null':
            if date_str >= '20160101' and date_str < '20160201':
                data_sets_by_month[1].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160201' and date_str < '20160301':
                data_sets_by_month[2].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160301' and date_str < '20160401':
                data_sets_by_month[3].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160401' and date_str < '20160501':
                data_sets_by_month[4].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160501' and date_str < '20160601':
                data_sets_by_month[5].write(",".join(X[i, :]) + "\n")
            elif date_str >= '20160601' and date_str < '20160701':
                data_sets_by_month[6].write(",".join(X[i, :]) + "\n")
    for i in xrange(1, 7):
        data_sets_by_month[i].close()

if __name__ == '__main__':
    #split_by_2_month_user('offline_2_month', 'ccf_offline_stage1_train.csv') 
    split_by_4_month_user(ccf_path + 'offline_4_month', 'ccf_offline_stage1_train.csv')
    split_by_4_month_user(ccf_path + 'online_4_month', 'ccf_online_stage1_train.csv')
