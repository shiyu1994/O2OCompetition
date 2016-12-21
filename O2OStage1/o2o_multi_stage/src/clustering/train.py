import numpy as np

my_path = '/Users/shiyu/Projects/o2o_multi_stage/my_data/'

def train():
    for i in xrange(6):
        m_features = np.genfromtxt(my_path + 'i_features_no_penalty_10_{0}.csv'.format(i + 2), delimiter=',', dtype=float)
        u_features = np.genfromtxt(my_path + 'u_features_no_penalty_10_{0}.csv'.format(i + 2), delimiter=',', dtype=float)
        kmeams_m = KMeans(10, m_features[:, 1:])
        kmeams_m.train(100)
        tags = kmeams_m.tag
        np.savetxt(my_path + 'm_tag_no_penalty_10_{0}.csv'.format(i + 2), np.hstack((np.array([m_features[:, 0]]).T, np.array([tags]).T)), fmt="%d,%d")

        kmeams_i = KMeans(10, u_features[:, 1:])
        kmeams_i.train(100)
        tags = kmeams_i.tag
        np.savetxt(my_path + 'u_tag_no_penalty_10_{0}.csv'.format(i + 2), np.hstack((np.array([u_features[:, 0]]).T, np.array([tags]).T)), fmt="%d,%d")

if __name__ == '__main__':
    train()
