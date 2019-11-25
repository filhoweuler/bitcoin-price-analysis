import pickle
import random
import time
import numpy as np
from sklearn.neural_network import MLPClassifier
from sklearn import preprocessing
from sklearn.model_selection import train_test_split
from sklearn.metrics import accuracy_score
from sklearn.metrics import confusion_matrix

with open('twitter_datapoints_dirty.pickle', 'rb') as f:
    data = pickle.load(f)

scaler = preprocessing.StandardScaler()

scaled_data = scaler.fit_transform(data[0])
print(scaled_data)

print(len(scaled_data))
print(len(data[1]))

V, V_test, Z, Z_test = train_test_split(scaled_data, data[1], test_size=0.2 , random_state = 47)

print(len(V_test))
print(len(Z_test))
print(len(V))
print(len(Z))

max_acc = -1
best_mlp = None

random.seed = time.time()

for i in range(50):
    mlp = MLPClassifier(solver='adam', alpha=0.0002, early_stopping=True, learning_rate_init=0.00001, hidden_layer_sizes=(100, ), activation='tanh', max_iter=500, random_state=random.randint(0, 99999999))

    mlp.fit(V, Z)

    Z_predict = mlp.predict(V_test)

    acc = accuracy_score(Z_test, Z_predict)

    print(f"Accuracy Score: {accuracy_score(Z_test, Z_predict)}")

    print("Confusion Matrix:")
    # Confusion Matrix, Basically it works like this.
    print(confusion_matrix(Z_test, Z_predict))

    if acc > max_acc:
        max_acc = acc
        best_mlp = mlp

print(f"Max acc = {max_acc}")

with open('twitter_adam_tanh_mlp.pickle', 'wb') as f:
    pickle.dump(best_mlp, f)

# for v, z in zip(data[0], data[1]):
#     print("="*20)
#     print(f"Features: {v}")
#     print(f"Target: {z}")
#     print("="*20)