import numpy as np

class CollaborativeFilter:
    def __init__(self, y, nu, ni, m=10, reg=0.5, lr=0.001, num_iter=500):
        self.num_iter = num_iter
        self.x = 0.01 * np.random.randn(nu, m)
        self.theta = 0.01 * np.random.randn(ni, m)
        self.y = y
        self.n_user = nu
        self.n_item = ni
        self.m = m
        self.lr = lr
        self.reg = reg
        self.n_date = y.shape[0]
        self.final_loss = 100
        print np.max(y)
        print np.min(y)

    def calc_loss(self, x, theta, y):   
        loss = 0.0
        n = self.n_date
        uid = y[:, 0]
        iid = y[:, 1]
        score = y[:, 2]
        loss += 0.5 * np.sum((np.sum(x[uid, :] * theta[iid, :], 1) - score) ** 2)
        loss += 0.5 * self.reg * np.sum(x * x) + 0.5 * self.reg * np.sum(theta * theta)
        loss /= n
        return loss

    def train(self):
        x = self.x
        theta = self.theta
        y = self.y
        lr = self.lr
        reg = self.reg

        n = len(y)
        dloss = 100
        loss = 1000000000
        for i in xrange(self.num_iter):
            dx = np.zeros_like(x)
            dtheta = np.zeros_like(theta)
            uid = np.zeros(self.n_date, dtype=int)
            iid = np.zeros(self.n_date, dtype=int)
            uid = y[:, 0]
            iid = y[:, 1]
            score = y[:, 2]
            dot = np.sum(x[uid, :] * theta[iid, :], 1)
            dxx = np.array([dot - score]).T * theta[iid, :]
            print "iter:", i
            #print "max(theta)", np.max(theta)
            #print "max(dxx)", np.max(dxx)
            for i in xrange(self.n_date):
                dx[uid[i], :] += dxx[i, :]
            #print "max(dx)", np.max(dx)
            dx += reg * x
            dx /= self.n_date
            #print "max(dx)", np.max(dx)
            x -= lr * dx
            dtt = np.array([dot - score]).T * x[uid, :]
            for i in xrange(self.n_date):
                dtheta[iid[i], :] += dtt[i, :]
            dtheta += reg * theta
            dtheta /= self.n_date
            #print "max(dtheta)", np.max(dtheta)
            theta -= lr * dtheta
            new_loss = self.calc_loss(x, theta, y)

            dloss = loss - new_loss
            loss = new_loss
            print "new_loss", new_loss
            if dloss < 0:
                break;
        self.final_loss = loss
        self.x = x
        self.theta = theta
