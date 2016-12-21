import numpy as np
from theano import *
import theano.tensor as T

def auc_score(prob, y):
    dots = np.zeros((len(prob)), dtype='f,f')
    all_pos = int(np.sum(y))
    all_neg = len(prob) - all_pos
    if all_neg == 0 or all_pos == 0:
        return -1
    prob_y = np.zeros(all_pos + all_neg, dtype='f,i4')
    for i in xrange(all_pos + all_neg):
        a = prob[i]
        b = y[i]
        prob_y[i] = (a, b)
    prob_y = np.sort(prob_y)
    true_pos = 0
    false_pos = 0
    pny = np.zeros((len(y), 3))
    cnt = -1
    curr_prob = -1
    for i in xrange(len(prob_y)):
        _prob, _y = prob_y[i]
        if _prob != curr_prob:
            cnt += 1
            pny[cnt, 0] = _prob
            curr_prob = _prob
        pny[cnt, 1] += _y
        pny[cnt, 2] += (1 - _y)
    for i in xrange(len(pny)):
        _prob, _pos, _neg = pny[len(pny) - 1 - i]
        true_pos += _pos
        false_pos += _neg
        aa = true_pos * 1.0 / all_pos
        bb = false_pos * 1.0 / all_neg
        assert aa >= 0 and aa<=1 and bb >= 0 and bb <= 1
        dots[i] = np.array([(bb, aa)], dtype='f,f')

    dots = np.sort(dots)
    area = 0.0
    for i in xrange(len(dots) - 1):
        x0, y0 = dots[i]
        x1, y1 = dots[i + 1]
        assert x0 >= 0 and x0 <= 1 and y0 >= 0 and y0 <= 1 and x1 >= 0 and x1 <= 1 and y1 >= 0 and y1 <= 1
        #if x1 - x0 >= 1e-4:
        area += (y0 + y1) / 2.0 * (x1 - x0)
    x0, y0 = dots[0]
    x1, y1 = dots[-1]
    area += y0 / 2 * x0
    area += (1 - y1) / 2 * (1 - x1)
    return area

def auc_score_avg(probs, ys, cids):
    cid_set_probs = {}
    cid_set_ys = {}
    l = len(cids)
    cnt = 0
    for i in xrange(l):
        if cids[i] != 'null' and cids[i] != 'fixed':
            if cids[i] not in cid_set_probs:
                cid_set_probs[cids[i]] = list()
                cid_set_ys[cids[i]] = list()
                cnt += 1
            cid_set_probs[cids[i]].append(probs[i])
            cid_set_ys[cids[i]].append(ys[i])
    score = 0.0
    cnt = 0
    for cid in cid_set_ys:
        area = auc_score(np.array(cid_set_probs[cid]), np.array(cid_set_ys[cid]))
        if area != -1:
            score += area
            cnt += 1
    return score / cnt

if __name__ == '__main__':
    predictions = T.fvector('predictions') 
    y = T.ivector('y')
    auc_func = theano.function([predictions, y], auc_score_avg(predictions, y), allow_input_downcast=True)
