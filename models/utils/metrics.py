import numpy as np
from sklearn.metrics import (
    accuracy_score,
    f1_score,
    confusion_matrix,
    classification_report,
    balanced_accuracy_score,
)

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
