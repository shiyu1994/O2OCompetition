from theano import *
import theano.tensor as T
import numpy as np
import lasagne
import code.metainfo.paths as paths

def multi_layer(input_var=None):
    input_layer = lasagne.layers.InputLayer((None, 489), input_var=input_var)
    drop_out1 = lasagne.layers.DropoutLayer(input_layer, 0.2)
    dense1 = lasagne.layers.DenseLayer(drop_out1, 1000)
    drop_out2 = lasagne.layers.DropoutLayer(dense1, 0.2)
    dense2 = lasagne.layers.DenseLayer(drop_out2, 2, nonlinearity=lasagne.nonlinearities.softmax)
    return dense2

def load_data(selected_features_file):
    selected_features = np.genfromtxt(paths.my_path + selected_features_file, delimiter=',', dtype=str)
    X_train = np.zeros((0, len(selected_features)), dtype=float)
    y_train = np.zeros(0, dtype=int)

    for i in xrange(3, 6):
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

    col_names = np.genfromtxt(paths.my_path + 'X{0}_col_names_train.csv'.format(6), delimiter=',', dtype=str)
    name_index = {}
    for j in xrange(col_names.shape[1]):
        name_index[col_names[0, j]] = int(col_names[1, j])
    selected_feature_indices = []
    for name in selected_features:
        selected_feature_indices.append(name_index[name])
    matrix = np.genfromtxt(paths.my_path + 'X{0}.csv'.format(6), delimiter=',', dtype=str)
    X_test = np.zeros((matrix.shape[0], len(selected_features)), dtype=float)
    X_test[:, :] = matrix[:, selected_feature_indices]

    y_test = np.zeros(matrix.shape[0], dtype=int)
    y_test[:] = matrix[:, name_index["y"]]

    return X_train, y_train, X_test, y_test

def iterate_minibatches(X, y, batchsize, shuffle):
    ndata = X.shape[0]
    for start in xrange(0, ndata, batchsize):
        yield X[start:start + batchsize, :], y[start:start + batchsize]

def train(selected_features_file, epochs=100):
    X_train, y_train, X_test, y_test = load_data(selected_features_file)
    #X_train = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 3, 3]], dtype=float)
    #y_train = np.array([0, 0, 1, 1], dtype=int)
    #X_test = np.array([[1, 2, 3], [1, 2, 3], [1, 2, 3], [1, 3, 3]], dtype=float)
    #y_test = np.array([0, 0, 1, 1], dtype=int)

    input_var = T.matrix('input')
    output_var = T.ivector('output')

    network = multi_layer(input_var)
    predictions = lasagne.layers.get_output(network)

    loss = lasagne.objectives.categorical_crossentropy(predictions, output_var)
    loss = loss.mean()

    params = network.get_params(trainable=True)
    updates = lasagne.updates.adam(loss, params, learning_rate=0.01)

    train_fn = theano.function([input_var, output_var], loss, updates=updates, allow_input_downcast=True)

    test_fn = theano.function([input_var, output_var], loss, allow_input_downcast=True)

    for i in xrange(epochs):
        train_loss = 0.0
        for batch in iterate_minibatches(X_train, y_train, 500, shuffle=True):
            X, y = batch
            train_loss += train_fn(X_train, y_train)
            print train_loss
        test_loss = test_fn(X_test, y_test)
        print "train_loss", train_loss
        print "test_loss", test_loss

if __name__ == '__main__':
    train("features.csv")
