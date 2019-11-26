import pandas as pd
import datetime
import random
import time
import numpy as np
import collections
import pickle
from sklearn.neural_network import MLPClassifier
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import precision_score
from sklearn.metrics import confusion_matrix

data = pd.HDFStore('BTC-USD_data.h5')

d = data['market']
data['market'] = d.query('index > 1518746400')
print(data['market'])
data['market'].drop('mid', axis=1)
data['market'].drop('volumefrom', axis=1)

# # Generate target vector
# target = []

# for i in range(len(data['market']) - 1):
#     current_closing = data['market'].iloc[i]['close']
#     next_closing = data['market'].iloc[i+1]['close']

#     if next_closing > current_closing:
#         target.append(1)
#     else:
#         target.append(-1)

# with open('data/test_market_target.pickle', 'wb') as f:
#     pickle.dump(target, f)
# Ends

with open('data/test_market_target.pickle', 'rb') as f:
    target = pickle.load(f)

# print(collections.Counter(target))
# input()

scaler = preprocessing.StandardScaler()

scaled_data = scaler.fit_transform(data['market'])
scaled_data = scaled_data[:len(scaled_data) - 1] # drop last row

data.close()

# print(scaled_data)

V, V_test, Z, Z_test = train_test_split(scaled_data, target, test_size=0.3)

random.seed = time.time()

max_acc = -1
best_mlp = None

max_pre = -1

for i in range(50):
    mlp = MLPClassifier(solver='adam', alpha=0.0002, learning_rate_init=0.00001, max_iter=500, early_stopping=True, hidden_layer_sizes=(55, ), activation='tanh')
    # mlp = MLPClassifier(solver='adam', alpha=0.0002, hidden_layer_sizes=(30, ), activation='tanh')

    mlp.fit(V, Z)

    Z_predict = mlp.predict(V_test)

    acc = accuracy_score(Z_test, Z_predict)
    print(f"Accuracy Score: {acc}")

    pre = precision_score(Z_test, Z_predict)
    print(f"Precision Score: {pre}")

    print("Confusion Matrix:")
    # Confusion Matrix, Basically it works like this.
    print(confusion_matrix(Z_test, Z_predict))

    if acc > max_acc:
        max_acc = acc
        best_mlp = mlp

    if pre > max_pre:
        max_pre = pre


print(f"Max acc = {max_acc}")
print(f"Max pre = {max_pre}")

print(len(V_test))
print(len(Z_test))
print(len(V))
print(len(Z))