import numpy as np
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    confusion_matrix,
    classification_report,
    balanced_accuracy_score,
)
from sklearn.utils import resample

def compute_metrics(all_labels, all_preds, id2label):
    # F1 macro (trọng tâm imbalance)
    f1_macro = f1_score(all_labels, all_preds, average='macro')

    # Avg Acc (mang tính phổ quát)
    avg_acc = accuracy_score(all_labels, all_preds)

    # balanced acc (acc trung bình trên các lớp)
    balanced_acc = balanced_accuracy_score(all_labels, all_preds)

    # acc per label (acc của từng lớp)
    cm = confusion_matrix(all_labels, all_preds, labels=list(id2label.keys()))
    acc_per_label = cm.diagonal() / (cm.sum(axis=1) + 1e-8)
    label_acc_dict = {id2label[i]: float(acc_per_label[i]) for i in id2label.keys()}

    # report
    report = classification_report(
        all_labels,
        all_preds,
        target_names=[id2label[i] for i in sorted(id2label.keys())],
        output_dict=True,
        zero_division=0,
    )

    return f1_macro, avg_acc, balanced_acc, label_acc_dict, report


def compute_bootstrap_ci(all_labels, all_preds, metric_name, n_bootstraps=1000, **kwargs):
    """Compute bootstrap confidence interval for a given metric function."""
    np.random.seed(59)

    labels_arr = np.array(all_labels)
    preds_arr = np.array(all_preds)
    n = len(labels_arr)

    if metric_name == "f1_macro":
        metric_func = f1_score
        if "average" not in kwargs:
            kwargs["average"] = "macro"
    elif metric_name == "balanced_acc":
        metric_func = balanced_accuracy_score
    else:
        raise ValueError(f"Invalid metric name: {metric_name}")

    bstr_scores = []
    
    for _ in range(n_bootstraps):
        indices = np.random.randint(0, n, size=n)
        score = metric_func(labels_arr[indices], preds_arr[indices], **kwargs)
        bstr_scores.append(score)

    # compute 95% confidence interval
    std_err = np.std(bstr_scores)
    ci_half_width = 1.96 * std_err
    return ci_half_width

