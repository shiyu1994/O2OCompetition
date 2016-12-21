import code.metainfo.paths as paths

def gen_manual_col_names():
    for k in xrange(3, 7):
        col_names = open(paths.my_path + "X{0}_col_names_train.csv".format(k), "w")
        for i in xrange(538):
            col_names.write("f{0},".format(i))
        col_names.write("y\n")
        for i in xrange(538):
            col_names.write("{0},".format(i))
        col_names.write("538\n")
        col_names.close()
    col_names = open(paths.my_path + "X{0}_col_names_train.csv".format(7), "w")
    for i in xrange(6):
        col_names.write("f{0},".format(i))
    for i in xrange(7, 537):
        col_names.write("f{0},".format(i))
    col_names.write("f{0}\n".format(537))
    for i in xrange(536):
        col_names.write("{0},".format(i))
    col_names.write("536\n")
    col_names.close()

    features = open(paths.my_path + "features.csv", "w")
    for i in xrange(7, 537):
        features.write("f{0},".format(i))
    features.write("f{0}".format(537)) 

if __name__ == '__main__':
    gen_manual_col_names()
