import numpy as np
import torch
import torch.nn as nn
import torch.nn.functional as F

###############################################################################################################
### Class-Balanced Weights (CBW) Calculation
###############################################################################################################

def get_cb_weights(label_stats, label2id, beta=0.9999):
    """Compute class-balanced weights for each label based on the effective number of samples.
    Args:
        label_stats (dict): A dictionary containing the count of samples for each label.
        label2id (dict): A dictionary mapping labels to their corresponding IDs.
        beta (float): The hyperparameter for class-balanced weights, typically close to 1. Default is 0.9999.
    Returns:
        torch.Tensor: A tensor containing the class-balanced weights for each label.
    """
    # Sort labels by their IDs to ensure correct order of weights
    n_classes = len(label2id.keys())
    samples_per_cls = [0] * n_classes
    for label, count in label_stats.items():
        if label in label2id:
            samples_per_cls[label2id[label]] = count

    # effective number of samples for each class (số lượng mẫu hiệu quả - En)
    samples_per_cls = np.array(samples_per_cls)
    effective_num = 1.0 - np.power(beta, samples_per_cls)

    # class-balanced weights (CBW) for each class
    weights = (1.0 - beta) / np.where(effective_num == 0, 1e-6, effective_num) # Add small epsilon to avoid division by zero

    # scale weights to ensure the sum of weights equals the number of classes
    weights = weights / np.sum(weights) * n_classes

    return torch.tensor(weights, dtype=torch.float)

################################################################################################################### GHM-C Loss Layer (Gradient Harmonized Mechanism)
################################################################################################################

class GHMC_Loss(nn.Module):
    def __init__(self, bins=10, momentum=0.7, cb_weights=None):
        super(GHMC_Loss, self).__init__()
        self.bins = bins
        self.momentum = momentum
        self.edges = [i / bins for i in range(bins + 1)]
        self.edges[-1] += 1e-6

        if momentum > 0:
            # Store gradient history
            self.register_buffer('acc_sum', torch.zeros(bins))

        self.cb_weights = cb_weights # class-balanced weights

    def forward(self, logits, targets):
        # Init targets as one-hot
        target_one_hot = F.one_hot(targets, num_classes=logits.size(-1)).float()
        
        # Compute gradients norm for each sample
        # g = ||p-y||_1
        with torch.no_grad():
            probs = torch.sigmoid(logits)
            g = torch.abs(probs - target_one_hot)

        # Compute GHM weights based on bins
        weights = torch.zeros_like(g)
        n = 0 # #bin store data
        tot = logits.size(0) * logits.size(1) # total of elem
        
        for i in range(self.bins):
            inds = (g >= self.edges[i]) & (g < self.edges[i+1])
            num_in_bin = inds.sum().item()
            if num_in_bin > 0:
                if self.momentum > 0:
                    self.acc_sum[i] = self.momentum * self.acc_sum[i] + (1 - self.momentum) * num_in_bin
                    weights[inds] = tot / self.acc_sum[i]
                else:
                    weights[inds] = tot / num_in_bin
                n+=1

        if n > 0:
            weights = weights / n

        # binary cross entropy loss
        loss = F.binary_cross_entropy_with_logits(logits, target_one_hot, reduction='none')

        # union CBW and GHM weights
        # GHM Weight * CB Weight * CE Loss
        if self.cb_weights is not None:
            final_weight = weights * self.cb_weights.to(logits.device).unsqueeze(0)
        else:
            final_weight = weights

        loss = (loss * final_weight).sum()/logits.size(0)

        return loss
