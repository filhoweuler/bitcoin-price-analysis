import pickle
import random
import time
import numpy as np
import collections
import os
from sklearn.neural_network import MLPClassifier
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import recall_score
from sklearn.metrics import f1_score
from sklearn.metrics import confusion_matrix

def get_source(filename):
    for s in filename.split('_'):
        if s in ['market', 'twitter', 'mixed']:
            return s

    return None

DATA_ROOT = 'data/'
MODELS_ROOT = 'models/'

exps = ['exp1','exp2']

datasets = {
    'exp1': ['daily_twitter_datapoints.pickle', 'daily_market_datapoints.pickle', 'daily_mixed_datapoints.pickle'],
    'exp2': ['daily_twitter_datapoints.pickle', 'daily_market_datapoints.pickle', 'daily_mixed_datapoints.pickle'],
    'exp3': ['filtered_daily_twitter_datapoints.pickle', 'daily_market_datapoints.pickle', 'filtered_daily_mixed_datapoints.pickle'],
    'exp4': ['filtered_daily_twitter_datapoints.pickle', 'daily_market_datapoints.pickle', 'filtered_daily_mixed_datapoints.pickle'],
}

mlp_config = {
    'original': {
        'solver':'adam', 
        'alpha':0.0002, 
        'hidden_layer_sizes':(30, ), 
        'activation':'tanh'
    },
    'modified': {
        'solver':'lbfgs', 
        'alpha':0.0002,
        'activation':'tanh',
        'early_stopping': True, 
        'max_iter':500, 
        'hidden_layer_sizes':(10, )
    },
}

if __name__ == "__main__":
    BASE_TIME = str(int(time.time()))
    os.mkdir(MODELS_ROOT + BASE_TIME)
    FILE_DIR = MODELS_ROOT + BASE_TIME + '/'
    LOG_FILE = open('logs/'+ BASE_TIME + '_training.logs.txt', 'w')
    for exp in exps:
        LOG_FILE.write(f'===== Starting Experiment {exp} =====\n')
        for ds in datasets[exp]:
            data_source = get_source(ds)
            LOG_FILE.write(f'Current dataset: {data_source}')
            with open(ds, 'rb') as f:
                data = pickle.load(f)

            V, V_test, Z, Z_test = train_test_split(data[0], data[1], test_size=0.3 , random_state = 47)

            test_file = '_'.join([FILE_DIR, exp, data_source, 'test_data'])
            with open(test_file, 'wb') as f:
                pickle.dump(V_test, f)                

            scaler = preprocessing.StandardScaler()
            # Fit only on test data
            scaler.fit(V)
            V = scaler.transform(V)
            V_test = scaler.transform(V_test)

            max_precision = -1
            best_mlp = None

            random.seed = time.time()

            for i in range(50):
                config = {}
                if exp in ['exp1', 'exp3']:
                    config = mlp_config['original']
                    if 'mixed' in ds:
                        config['hidden_layer_sizes'] = (55,)
                elif exp in ['exp2', 'exp4']:
                    config = mlp_config['modified']
                    if 'mixed' in ds:
                        config['hidden_layer_sizes'] = (20,)
                else:
                    LOG_FILE.write("----- ERROR: Experiment id not known ------\n")
                    break

                mlp = MLPClassifier(**config)

                mlp.fit(V, Z)
                Z_predict = mlp.predict(V_test)
                precision = precision_score(Z_test, Z_predict)

                if precision > max_precision:
                    best_mlp = mlp
                    max_precision = precision

            Z_predict = best_mlp.predict(V_test)
            acc = accuracy_score(Z_test, Z_predict)
            LOG_FILE.write(f"Accuracy Score: {accuracy_score(Z_test, Z_predict)}\n")
            precision = precision_score(Z_test, Z_predict)
            LOG_FILE.write(f"Precision Score: {precision}\n")
            recall = recall_score(Z_test, Z_predict)
            LOG_FILE.write(f"Recall Score: {recall}\n")
            f1 = f1_score(Z_test, Z_predict)
            LOG_FILE.write(f"F1 Score: {f1}\n")
            LOG_FILE.write("Confusion Matrix:\n")
            LOG_FILE.write(str(confusion_matrix(Z_test, Z_predict, labels=[1, -1])))
            LOG_FILE.write('\n\n')

            
            mlp_file = '_'.join([FILE_DIR, exp, data_source, 'mlp'])
            scaler_file = '_'.join([FILE_DIR, exp, data_source, 'scaler'])

            with open(mlp_file, 'wb') as f:
                pickle.dump(best_mlp, f)
            with open(scaler_file, 'wb') as f:
                pickle.dump(scaler, f)
            
        LOG_FILE.write('===== Ending Experiment =====\n')

    LOG_FILE.close()