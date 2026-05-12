import os
import re
import polars as pl
import pandas as pd
import numpy as np
import torch
import torch.nn as nn
from pyvi import ViTokenizer
from transformers import (
    AutoTokenizer,
    AutoModelForSequenceClassification,
    TrainingArguments,
    Trainer,
)
from datasets import Dataset, load_dataset, Features, Value, ClassLabel
from sklearn.metrics import accuracy_score, f1_score, classification_report

#############################################################################################
#### CONFIGURATION                                                                      #####
#############################################################################################

BATCH_SIZE = 500_000
TRAIN_EPOCHS = 2
LABELED_FILE = "ft_seed_+uit.parquet" # Change this to new fine-tuned dataset
MODEL_NAME = "uitnlp/visobert"
MODEL_PATH = "./results/best_model"
OUTPUT_DIR = "./results"
IS_TUNED = os.path.exists(MODEL_PATH)
USE_LOCAL = False

LABEL2ID = {
    "admiration": 0, "amusement": 1, "anger": 2, "annoyance": 3,
    "approval": 4, "caring": 5, "confusion": 6, "curiosity": 7,
    "desire": 8, "disappointment": 9, "disapproval": 10, "disgust": 11,
    "embarrassment": 12, "excitement": 13, "fear": 14, "gratitude": 15,
    "grief": 16, "joy": 17, "love": 18, "nervousness": 19,
    "optimism": 20, "pride": 21, "realization": 22, "relief": 23,
    "remorse": 24, "sadness": 25, "surprise": 26, "neutral": 27,
}

ID2LABEL = {id: label for label, id in LABEL2ID.items()}
NUM_CLASSES = len(LABEL2ID)

#############################################################################################
#### MODEL SETUP                                                                        #####
#############################################################################################

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_NAME if not IS_TUNED or not USE_LOCAL else MODEL_PATH,
    num_labels=NUM_CLASSES,
    label2id=LABEL2ID,
    id2label=ID2LABEL,
    problem_type="single_label_classification" 
)

def compute_metrics(eval_pred):
    logits = eval_pred.predictions
    labels = eval_pred.label_ids

    if isinstance(logits, tuple):
        logits = logits[0]

    predictions = np.argmax(logits, axis=-1)
    # Vì bài toán là single-label, labels đã là số nguyên (index), không dùng argmax nữa
    references = labels

    acc = accuracy_score(references, predictions)
    f1_mac = f1_score(references, predictions, average="macro", zero_division=0)

    return {
        "accuracy": acc,
        "f1_macro": f1_mac
    }

def tokenize_function(examples):
    texts = examples["text"]
    return tokenizer(
        texts, padding="max_length", truncation=True, max_length=256
    )

def norm_label(batch):
    cleaned_ids = []
    for label_str_orig in batch["labels"]:
        label_str = str(label_str_orig).strip().lower()
        label_str = label_str.split("/")[0].strip()
        label_str = re.sub(r"\s*\(.*?\)", "", label_str).strip()
        label_id = LABEL2ID.get(label_str, LABEL2ID["neutral"])
        
        # Trả về nhãn số nguyên trực tiếp
        cleaned_ids.append(int(label_id))
        
    batch["labels"] = cleaned_ids
    return batch

####################################################################################################
#### DATA PROCESSING                                                                           #####
####################################################################################################

LABELED_FILENAME, extension = os.path.splitext(LABELED_FILE)
if extension == ".csv":
    labeled_lf = pl.scan_csv(LABELED_FILE)
    labeled_lf.sink_parquet(f"{LABELED_FILENAME}.parquet", row_group_size=BATCH_SIZE)

full_dataset = load_dataset(
    "parquet", data_files=f"{LABELED_FILENAME}.parquet", split="train"
)
full_dataset = full_dataset.map(norm_label, batched=True)

# Khai báo Features dưới dạng ClassLabel để Hugging Face hiểu đây là Single-Label
new_features = full_dataset.features.copy()
new_features["labels"] = ClassLabel(num_classes=NUM_CLASSES, names=list(LABEL2ID.keys()))
full_dataset = full_dataset.cast(new_features)

split_datasets = full_dataset.train_test_split(test_size=0.05, seed=42)
train_dataset = split_datasets["train"].map(tokenize_function, batched=True)
eval_dataset = split_datasets["test"].map(tokenize_function, batched=True)

####################################################################################################
#### CLASS WEIGHTS (XỬ LÝ MẤT CÂN BẰNG)                                                        #####
####################################################################################################

# Calc Class Weights based on Train Dataset Distribution
train_labels_list = train_dataset["labels"]
label_counts = np.bincount(train_labels_list, minlength=NUM_CLASSES)
total_samples = len(train_labels_list)

class_weights = total_samples / (NUM_CLASSES * (label_counts + 1e-9))
class_weights[label_counts == 0] = 0.0 # An toàn chống chia cho 0 với nhãn thiếu

device = torch.device('cuda' if torch.cuda.is_available() else 'cpu')
class_weights_tensor = torch.tensor(class_weights, dtype=torch.float32).to(device)

#############################################################################################
#### CUSTOM TRAINER (USING LAYER WEIGHT)                                               #####
#############################################################################################

class CustomTrainer(Trainer):
    def compute_loss(self, model, inputs, return_outputs=False, **kwargs):
        labels = inputs.get("labels") 
        outputs = model(**inputs)
        logits = outputs.get("logits") 
        
        # Apply CrossEntropyLoss with Label Smoothing and Class Weights
        loss_fct = nn.CrossEntropyLoss(
            weight=class_weights_tensor, 
            label_smoothing=self.args.label_smoothing_factor
        )
        
        loss = loss_fct(logits.view(-1, self.model.config.num_labels), labels.view(-1))
        
        return (loss, outputs) if return_outputs else loss

#############################################################################################
#### TRAINING                                                                           #####
#############################################################################################

training_args = TrainingArguments(
    output_dir="./results",
    learning_rate=5e-5,
    fp16=True,
    per_device_train_batch_size=96,
    gradient_accumulation_steps=4,
    per_device_eval_batch_size=32,
    dataloader_num_workers=12,
    dataloader_pin_memory=True,
    label_smoothing_factor=0.1, 
    lr_scheduler_type="cosine",
    warmup_ratio=0.1,
    num_train_epochs=TRAIN_EPOCHS,
    weight_decay=0.01,
    eval_strategy="epoch",
    save_strategy="epoch",
    save_total_limit=2,
    load_best_model_at_end=True,
    metric_for_best_model="f1_macro",
)

trainer = CustomTrainer(
    model=model,
    args=training_args,
    train_dataset=train_dataset,
    eval_dataset=eval_dataset,
    processing_class=tokenizer,
    compute_metrics=compute_metrics,
)

trainer.train()
trainer.save_model("./results/best_model")

#############################################################################################
#### EVALUATION REPORT                                                                  #####
#############################################################################################

print("\n" + "="*50)
print("CLASSIFICATION REPORT")
print("="*50)

predictions, labels, _ = trainer.predict(eval_dataset)

# Extract predictions via argmax
if isinstance(predictions, tuple):
    predictions = predictions[0]
preds_flat = np.argmax(predictions, axis=-1)
labels_flat = labels 

target_names = [ID2LABEL[i] for i in range(NUM_CLASSES)]

# Print classification report
report = classification_report(
    labels_flat, 
    preds_flat, 
    target_names=target_names, 
    zero_division=0,
    digits=4
)
print(report)
