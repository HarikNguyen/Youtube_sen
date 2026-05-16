class EarlyStopping:
    def __init__(self, patience=3, delta=0.0):
        """
        Args:
            patience (int): The number of epochs to wait for improvement.
            delta (float): The minimum change in the monitored quantity to qualify as an improvement.
        """
        self.patience = patience
        self.delta = delta
        self.counter = 0
        self.best_score = None
        self.early_stop = False

    def __call__(self, current_score):
        if self.best_score is None:
            self.best_score = current_score
        elif current_score < self.best_score + self.delta:
            self.counter += 1
            print(f"\n[EarlyStopping] Patience: {self.counter}/{self.patience} (Best: {self.best_score:.4f})")
            if self.counter >= self.patience:
                self.early_stop = True
        else:
            self.best_score = current_score
            self.counter = 0 # Reset counter if improvement is detected
