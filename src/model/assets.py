from tensorflow import keras
from tensorflow.keras import layers
from tensorflow.keras import metrics
from tensorflow.keras import regularizers
from tensorflow.keras.optimizers import Adam
from keras_tuner import HyperModel
from common.utils import unpickle_me


class KaladinData:
    def __init__(self, days, tc, use_eval, data_dct=None):
        self.days = days
        self.tc = tc

        # Read files from disk
        if data_dct is None:
            data_dct = unpickle_me('input_data/use_eval_{}/data_dct_tc{}_days{}.pkl'.format(use_eval, tc, days))
        else:
            data_dct = data_dct[(tc, days)]

        # Configure convolutional branch data
        self.dimensions = data_dct['dimensions']
        self.conv_dimensions = [dim for dim in self.dimensions if dim != 'layer']
        self.conv_train_inputs = [data_dct['train'][dim] for dim in self.conv_dimensions]
        self.conv_valid_inputs = [data_dct['valid'][dim] for dim in self.conv_dimensions]
        self.conv_test_inputs = [data_dct['test'][dim] for dim in self.conv_dimensions]

        # Overall inputs
        self.train_inputs = self.conv_train_inputs 
        self.valid_inputs = self.conv_valid_inputs
        self.test_inputs = self.conv_test_inputs

        # Configure dense branch data
        if use_eval:
            self.dense_train_input = data_dct['train']['layer']
            self.dense_train_input = self.dense_train_input[:, :, 1:].reshape(
                [self.dense_train_input.shape[0], self.dense_train_input.shape[1]])
            self.train_inputs.append(self.dense_train_input)

            self.dense_valid_input = data_dct['valid']['layer'] 
            self.dense_valid_input = self.dense_valid_input[:, :, 1:].reshape(
                [self.dense_valid_input.shape[0], self.dense_valid_input.shape[1]])
            self.valid_inputs.append(self.dense_valid_input)

            self.dense_test_input = data_dct['test']['layer'] 
            self.dense_test_input = self.dense_test_input[:, :, 1:].reshape(
                [self.dense_test_input.shape[0], self.dense_test_input.shape[1]])
            self.test_inputs.append(self.dense_test_input)
        
        else:
            self.dense_train_input, self.dense_valid_input, self.dense_test_input = None, None, None

        # Overall inputs/outputs
        self.train_user_list = data_dct['train_user_list']
        self.valid_user_list = data_dct['valid_user_list']
        self.test_user_list = data_dct['test_user_list']
        self.train_labels = data_dct['train_labels']
        self.valid_labels = data_dct['valid_labels']
        self.test_labels = data_dct['test_labels']

        # misc
        self.insights_df = data_dct['df']

class KaladinModel(HyperModel):
    def __init__(self, data):
        self.data = data

    def build(self, hp):
        # Define tune-able hyperparameters
        num_filters = hp.Int('num_filters', 8, 32, 8)
        filter_count_modifiers = [
            hp.Int('fltr_mod_{}'.format(dim),1, 3, 1) 
            for dim in self.data.conv_dimensions]
        big_kernels = hp.Boolean('big_kernels')
        batch_norm = hp.Boolean('batch_norm')
        num_layers = hp.Int('num_layers', 1, 3, 1)
        dense_branch_node_factor = hp.Int('dense_branch_node_factor', 1, 3, 1)
        reg_l1 = hp.Float('reg_l1', 0.000001, 0.01, sampling='log')
        reg_l2 = hp.Float('reg_l2', 0.000001, 0.01, sampling='log')
        output_layer_1_size = hp.Int('output_layer_1_size', 32, 512, 32)
        output_layer_2_size = hp.Int('output_layer_2_size', 32, 256, 32)
        output_layer_3_size = hp.Int('output_layer_3_size', 8, 64, 8)
        learning_rate = hp.Choice("learning_rate", values=[0.001, 0.0002])

        # Build Conv2D branches, remember input layer and tensors
        tensors, input_layers = [], []
        for i, dim in enumerate(self.data.conv_dimensions):
            input_layer, tensor = self.build_model_2d_branch(
                self.data.conv_train_inputs[i],
                num_filters=num_filters*filter_count_modifiers[i],
                num_layers = num_layers,
                big_kernels=big_kernels,
                batch_norm=batch_norm) 
            tensors.append(tensor)
            input_layers.append(input_layer)

        if self.data.dense_train_input is not None:
            # Dense layer branch
            input_layer_denselayer, x_denselayer = self.build_dense_branch(
                self.data.dense_train_input, 
                [16*dense_branch_node_factor, 4*dense_branch_node_factor],
                reg_l1, 
                reg_l2)
            
            tensors.append(x_denselayer)
            input_layers.append(input_layer_denselayer)

        # Combine branch outputs
        x = layers.Concatenate()(tensors)

        # Add some final dense layers, then an output layer
        x = layers.Dense(output_layer_1_size, activation='relu', kernel_regularizer=regularizers.l1_l2(l1=reg_l1, l2=reg_l2))(x)
        x = layers.Dense(output_layer_2_size, activation='relu', kernel_regularizer=regularizers.l1_l2(l1=reg_l1, l2=reg_l2))(x)
        x = layers.Dense(output_layer_3_size, activation='relu', kernel_regularizer=regularizers.l1_l2(l1=reg_l1, l2=reg_l2))(x)
        x = layers.Dense(1, activation='sigmoid')(x)

        # Compile the model
        model = keras.Model(inputs=input_layers, outputs=x, name='kaladin')
        model.compile(
            optimizer=Adam(learning_rate), 
            loss='binary_crossentropy', 
            metrics=[
                metrics.AUC(name='auc'), 
                metrics.Precision(name='p'), # tp/(tp+fp)
                metrics.Recall(name='r'),    # tp/(tp+fn)
                metrics.BinaryAccuracy('acc')]
        )
        return model

    def build_dense_branch(self, input_data, layer_dims, reg_l1, reg_l2):
        """
        Expects input_data in shape
        (num_samples, num_cols)
        """
        input_layer = keras.Input(shape=input_data.shape[1:])
        x = input_layer
        
        for layer_dim in layer_dims:
            x = layers.Dense(
                layer_dim, 
                activation='relu', 
                kernel_regularizer=regularizers.l1_l2(l1=reg_l1, l2=reg_l2)
            )(x)
        
        return input_layer, x

    def build_model_2d_branch(self, input_data, num_filters=30, num_layers=2, 
                          big_kernels=True, batch_norm=False):
        """
        Expects input_data in shape
        (num_samples, num_insights, 2, time_steps)
        """
        input_layer = keras.Input(shape=input_data.shape[1:])
        if big_kernels:
            ksize = (1, 2)
        else:
            ksize = (1, 1)
        
        x = layers.Convolution2D(
            filters=int(num_filters), 
            kernel_size=ksize,
            strides=1,
            activation='relu'
        )(input_layer)
        for n in range(num_layers-1):
            x = layers.Convolution2D(
                filters=int(num_filters/(n+2) + 1), 
                kernel_size=1,
                strides=1,
                activation='relu'
            )(x)
        if batch_norm:
            x = layers.BatchNormalization()(x)
            
        x = layers.Flatten()(x)
        return input_layer, x