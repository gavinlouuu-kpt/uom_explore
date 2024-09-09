import torch
import torch.nn as nn
import torch.optim as optim
import torch.nn.functional as F
from torch.utils.data import DataLoader, TensorDataset
from sklearn.model_selection import train_test_split
from sklearn.preprocessing import StandardScaler
from sklearn.metrics import confusion_matrix
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import json

spk_json = "D:\\code\\uom_explore\\data_science\\scripts\\parameter.json"

def load_params(param_path):
    with open(param_path, 'r') as file:
        params = json.load(file)
    return params

def load_data(data_path):
    df = pd.read_csv(data_path)
    return df

def augment_data(X, y, noise_factor=0.05, num_augmentations=1):
    augmented_X = [X]
    augmented_y = [y]
    
    for _ in range(num_augmentations):
        noise = torch.randn_like(X) * noise_factor
        X_augmented = X + noise
        augmented_X.append(X_augmented)
        augmented_y.append(y)
    
    return torch.cat(augmented_X), torch.cat(augmented_y)

def preprocess_data(df, ground_truth, heaters_to_keep, bmes_to_keep, sensor_features=None):
    all_columns = df.columns.tolist()
    features = [col for col in all_columns if col != 'experiment_id' and col != ground_truth]

    # Separate heater features and BME features
    heater_features = [feature for feature in features if feature.split('_')[-1].isdigit()]
    bme_features = [feature for feature in features if not feature.split('_')[-1].isdigit()]
    
    # Filter heater features based on settings_to_keep and configurations
    heater_features_to_keep = []
    for heater in heaters_to_keep:
        if sensor_features:
            heater_features_to_keep.extend([f"{config}_{heater}" for config in sensor_features])
        else:
            heater_features_to_keep.extend([feature for feature in heater_features 
                                            if feature.endswith(f"_{heater}")])

    # Filter BME features (excluding '_std' features)
    bmes_to_keep_tuple = tuple(bmes_to_keep)
    bme_features_to_keep = [col for col in bme_features if col.endswith(bmes_to_keep_tuple)]
    
    # Combine filtered heater and BME features
    features_to_keep = heater_features_to_keep + bme_features_to_keep
    
    X = df[features_to_keep]
    input_size = len(features_to_keep)

    return X, input_size
    
def split_and_scale_data(X, y, params):
    random_state = params['random_state']
    batch_size = params['mlp']['batch_size']

    # Split into training, validation, and test sets
    X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=random_state)
    X_train, X_val, y_train, y_val = train_test_split(X_train, y_train, test_size=0.25, random_state=random_state)  # This makes 60%, 20%, 20%

    # Initialize the StandardScaler
    scaler = StandardScaler()

    # Fit the scaler to the training data and transform it
    X_train_scaled = scaler.fit_transform(X_train)

    # Apply the same transformation to validation and test sets
    X_val_scaled = scaler.transform(X_val)
    X_test_scaled = scaler.transform(X_test)

    # Convert arrays to tensors
    X_train_scaled = torch.tensor(X_train_scaled, dtype=torch.float32)
    y_train = torch.tensor(y_train.to_numpy(), dtype=torch.long)
    X_val_scaled = torch.tensor(X_val_scaled, dtype=torch.float32)
    y_val = torch.tensor(y_val.to_numpy(), dtype=torch.long)
    X_test_scaled = torch.tensor(X_test_scaled, dtype=torch.float32)
    y_test = torch.tensor(y_test.to_numpy(), dtype=torch.long)

    # Create datasets
    train_dataset = TensorDataset(X_train_scaled, y_train)
    val_dataset = TensorDataset(X_val_scaled, y_val)
    test_dataset = TensorDataset(X_test_scaled, y_test)

    # Data loaders
    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)
    val_loader = DataLoader(val_dataset, batch_size=batch_size, shuffle=False)
    test_loader = DataLoader(test_dataset, batch_size=batch_size, shuffle=False)

    # Use the augmentation in your data preparation
    if params['mlp']['augmentation']['enabled']:
        X_train_augmented, y_train_augmented = augment_data(
            X_train_scaled, 
            y_train, 
            noise_factor=params['mlp']['augmentation']['noise_factor'],
            num_augmentations=params['mlp']['augmentation']['num_augmentations']
        )
        train_dataset = TensorDataset(X_train_augmented, y_train_augmented)
    else:
        train_dataset = TensorDataset(X_train_scaled, y_train)

    train_loader = DataLoader(train_dataset, batch_size=batch_size, shuffle=True)

    return train_loader, val_loader, test_loader

class MLPClassifier(nn.Module):
    def __init__(self, input_size, hidden_sizes, num_classes, dropout_prob):
        super(MLPClassifier, self).__init__()
        self.layers = nn.ModuleList()
        self.batch_norms = nn.ModuleList()
        self.dropout_prob = dropout_prob
        
        # Input layer
        self.layers.append(nn.Linear(input_size, hidden_sizes[0]))
        self.batch_norms.append(nn.BatchNorm1d(hidden_sizes[0]))

        # Hidden layers
        for i in range(len(hidden_sizes) - 1):
            self.layers.append(nn.Linear(hidden_sizes[i], hidden_sizes[i + 1]))
            self.batch_norms.append(nn.BatchNorm1d(hidden_sizes[i+1]))
        
        # Output layer
        self.layers.append(nn.Linear(hidden_sizes[-1], num_classes))
        
        # Softmax activation for the output layer
        # self.softmax = nn.Softmax(dim=1)
    
    def forward(self, x):
        for i in range(len(self.layers) - 1):
            x = torch.relu(self.batch_norms[i](self.layers[i](x)))
            x = F.dropout(x, p=self.dropout_prob, training=self.training)
        x = self.layers[-1](x)
        # x = self.softmax(x)
        return x

def calculate_confusion_matrix(model, data_loader, num_classes):
    model.eval()
    all_preds = []
    all_labels = []
    with torch.no_grad():
        for X, y in data_loader:
            outputs = model(X)
            _, preds = torch.max(outputs, 1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(y.cpu().numpy())
    
    return confusion_matrix(all_labels, all_preds)

def plot_confusion_matrix(cm, class_names):
    plt.figure(figsize=(10, 8))
    sns.heatmap(cm, annot=True, fmt='d', cmap='Blues', xticklabels=class_names, yticklabels=class_names)
    plt.title('Confusion Matrix')
    plt.ylabel('True label')
    plt.xlabel('Predicted label')
    plt.show()

def predict(model, data):
    model.eval()
    with torch.no_grad():
        output = model(data)
        _, predicted_class = torch.max(output, dim=1)
    return predicted_class

# Function to compute the accuracy
def calculate_accuracy(y_pred, y_true):
    _, predicted = torch.max(y_pred, dim=1)  # Get the index of the max log-probability
    correct = (predicted == y_true).float().sum()
    return correct / y_true.shape[0]

def plot_losses(train_losses, val_losses):
    plt.figure(figsize=(12, 5))
    plt.plot(train_losses, label='Training Loss')
    plt.plot(val_losses, label='Validation Loss')
    plt.xlabel('Epochs')
    plt.ylabel('Loss')
    plt.title('Training and Validation Loss')
    plt.legend()
    plt.show()

def plot_accuracies(train_accuracies, val_accuracies):
    plt.figure(figsize=(12, 5))
    plt.plot(train_accuracies, label='Training Accuracy')
    plt.plot(val_accuracies, label='Validation Accuracy')
    plt.xlabel('Epochs')
    plt.ylabel('Accuracy')
    plt.title('Training and Validation Accuracy')
    plt.legend()
    plt.show()

def train_and_evaluate(model, criterion, optimizer, scheduler, scheduler_params, num_classes, train_loader, val_loader, epochs=10, patience=5):
    train_losses = []
    val_losses = []
    train_accuracies = []
    val_accuracies = []

    best_val_loss = float('inf')
    best_model = None
    counter = 0

    for epoch in range(epochs):
        model.train()
        train_loss = 0
        train_accuracy = 0
        for X_batch, y_batch in train_loader:
            optimizer.zero_grad()
            y_pred = model(X_batch)
            loss = criterion(y_pred, y_batch)
            loss.backward()
            optimizer.step()

            train_loss += loss.item()
            train_accuracy += calculate_accuracy(y_pred, y_batch)
        
        train_loss /= len(train_loader)
        train_accuracy /= len(train_loader)
        
        model.eval()
        val_loss = 0
        val_accuracy = 0
        with torch.no_grad():
            for X_val, y_val in val_loader:
                y_val_pred = model(X_val)
                val_loss += criterion(y_val_pred, y_val).item()
                val_accuracy += calculate_accuracy(y_val_pred, y_val)
        
        val_loss /= len(val_loader)
        val_accuracy /= len(val_loader)
        
        if scheduler_params['enabled']:
            scheduler.step(val_loss)

        train_losses.append(train_loss)
        val_losses.append(val_loss)
        train_accuracies.append(train_accuracy)
        val_accuracies.append(val_accuracy)
        
        print(f'Epoch {epoch+1}, Train Loss: {train_loss:.4f}, Train Accuracy: {train_accuracy:.4f}, '
              f'Validation Loss: {val_loss:.4f}, Validation Accuracy: {val_accuracy:.4f}')

        # # Early stopping logic
        # if val_loss < best_val_loss:
        #     best_val_loss = val_loss
        #     best_model = model.state_dict()
        #     counter = 0
        # else:
        #     counter += 1
        #     if counter >= patience:
        #         print(f"Early stopping triggered after {epoch+1} epochs")
        #         model.load_state_dict(best_model)
        #         break
    cm = calculate_confusion_matrix(model, val_loader, num_classes=num_classes)
    plot_confusion_matrix(cm, class_names=['Class ' + str(i) for i in range(num_classes)])
    plot_losses(train_losses, val_losses)
    plot_accuracies(train_accuracies, val_accuracies)

    return model, train_losses, val_losses, train_accuracies, val_accuracies


def main():
    
    param_path = spk_json

    params = load_params(param_path)

    data_path = params['data_paths']['debruijn_1']
    hidden_size = params['mlp']['hidden_size']
    ground_truth = params['ground_truth']
    num_epochs = params['mlp']['num_epochs']
    batch_size = params['mlp']['batch_size']
    learning_rate = params['mlp']['learning_rate']
    momentum_value = params['mlp']['momentum_value']
    dropout_rate = params['mlp']['dropout']
    weight_decay = params['mlp']['weight_decay']
    random_state = params['random_state']
    scheduler_params = params['mlp']['scheduler']

    df = load_data(data_path)
    y = df[ground_truth]
    num_classes = df[ground_truth].nunique()
    heaters_to_keep = params['feature_selection']['heaters_to_keep']
    bmes_to_keep = params['feature_selection']['bmes_to_keep']
    sensor_features = params['feature_selection'].get('sensor_features', None)
    X, input_size = preprocess_data(df, ground_truth, heaters_to_keep, bmes_to_keep, sensor_features)

    print("Features:")
    print(json.dumps(X.columns.tolist(), indent=2))
    print(f"New input size: {input_size}")

    train_loader, val_loader, test_loader = split_and_scale_data(X, y, params)

    # Initialize the model
    model = MLPClassifier(input_size, hidden_size, num_classes, dropout_rate)

    criterion = nn.CrossEntropyLoss()
    optimizer = optim.Adam(model.parameters(), lr=learning_rate, weight_decay=weight_decay)
    # Use learning rate scheduler
    scheduler = optim.lr_scheduler.ReduceLROnPlateau(
        optimizer, 
        mode='min', 
        patience=scheduler_params['patience'],
        factor=scheduler_params['factor'],
        threshold=scheduler_params['threshold'],
        cooldown=scheduler_params['cooldown'],
        min_lr=scheduler_params['min_lr']
    )

    model, train_losses, val_losses, train_accuracies, val_accuracies = train_and_evaluate(model, criterion, optimizer, scheduler, scheduler_params, num_classes, train_loader, val_loader, num_epochs, patience=5)

if __name__ == "__main__":
    main()

