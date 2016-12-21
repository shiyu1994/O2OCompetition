import numpy as np

class KMeans(object): 
    def __init__(self, num_class, data):
        self.data = data
        self.n_data = data.shape[0]
        self.num_class = num_class
        self.tag = np.zeros(self.n_data, dtype=int)

    def train(self, num_iter):
        data = self.data
        centers = data[np.random.choice(self.n_data, self.num_class), :]

        for k in xrange(num_iter):
            new_centers = np.zeros_like(centers, dtype=float)
            class_counter = np.zeros(self.num_class, dtype=int)
            mean_dis = 0
            for i in xrange(self.n_data):
                min_dis = 1000000
                data_i = data[i, :]
                data_norm = np.sqrt(np.sum(data_i * data_i))
                for j in xrange(self.num_class):
                    dis = np.sum((data_i - centers[j, :]) ** 2)
                    if dis < min_dis:
                        min_dis = dis
                        self.tag[i] = j
                new_centers[self.tag[i], :] += data_i
                class_counter[self.tag[i]] += 1
                mean_dis += min_dis / self.n_data

            for i in xrange(self.num_class):
                new_centers[i, :] /= class_counter[i]

            centers = new_centers

            print "iter", k, mean_dis
