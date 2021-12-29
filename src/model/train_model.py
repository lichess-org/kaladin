from model.assets import KaladinData, KaladinModel
from shap_values import prepare_shap_explainer_data
from keras_tuner import Hyperband, Objective
from tensorflow.keras.callbacks import EarlyStopping
import matplotlib.pyplot as plt
import numpy as np
import os

def train_model(days, tc, use_eval):
    # Read in data
    data = KaladinData(days, tc, use_eval)

    # Build model
    model = KaladinModel(data)

    # Tune hyper-parameters
    tuner = Hyperband(
        hypermodel=model,
        objective=Objective('val_loss', direction='min'),
        max_epochs=300,
        factor=3,
        hyperband_iterations=1,
        seed=37,
        project_name='kaladin_tuning_{}_{}_{}'.format(use_eval, days, tc))

    early_stopping = EarlyStopping(
        monitor='val_loss',
        min_delta=0.001,
        patience=15,
        mode='min',
        verbose=1,
        restore_best_weights=True
    )
    tuner.search(
        x=data.train_inputs,
        y=data.train_labels,
        batch_size=256,
        epochs=500,
        callbacks=[early_stopping],
        verbose=1,
        validation_data=(
            data.valid_inputs, 
            data.valid_labels)
        )
    
    # Remember best hyperparameters
    best_hyperparameters = tuner.get_best_hyperparameters()[0]
    tuned_model = model.build(best_hyperparameters)

    # Re-train model using best hyperparameters on training and validation data
    combined_inputs = []
    for i in range(len(data.train_inputs)):
        combined_inputs.append(np.concatenate([data.train_inputs[i], data.valid_inputs[i]]))

    history = tuned_model.fit(
        x=combined_inputs,
        y=np.concatenate([data.train_labels, data.valid_labels]),
        batch_size=256,
        epochs=500,
        callbacks=[early_stopping],
        verbose=1,
        validation_data=(data.test_inputs, data.test_labels)
    )

    # Write tuned model to disk
    directory = 'model/eval{}_tc{}_days{}/'.format(use_eval, tc, days)
    if not os.path.exists(directory):
        os.makedirs(directory)
    tuned_model.save(directory+'model.SavedModel')

    # Keep a plot of training history
    fig=plt.figure(figsize=(12,8), dpi= 300, facecolor='w', edgecolor='k')
    plt.plot(history.history['val_auc'])
    plt.plot(history.history['auc'])
    plt.title('Training history')
    plt.ylabel('Score')
    plt.xlabel('epoch')
    plt.legend(['val_auc', 'auc'], loc='upper left')
    plt.grid()
    plt.savefig(directory+'training_history.png')

    # Prepare data for shap explainer
    prepare_shap_explainer_data(data, directory)

if __name__ == '__main__':
    train_model(180, 6, 0)
    train_model(180, 2, 0)