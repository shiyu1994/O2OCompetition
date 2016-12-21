import numpy as np
import code.metainfo.paths as paths

def query(fid):
    col_names = np.genfromtxt(paths.my_path + 'X3_col_names.csv', delimiter=',', dtype=str)
    print col_names.shape[1]
    for i in xrange(7, col_names.shape[1]):
        if int(col_names[1, i]) - fid == 7:
            print col_names[0, i]
            break

if __name__ == '__main__':
    query(142)
