import os
import time
import torch
import polars as pl
from pyvi import ViTokenizer
from tqdm import tqdm
from torch.utils.data import DataLoader, Dataset
from torch.cuda.amp import autocast
from transformers import AutoTokenizer, AutoModelForSequenceClassification
import torch.nn.functional as F

# --- SETTINGS ---
CHUNK_SIZE = 1_000_000
MINI_BATCH_SIZE = 2048    
PREDICT_FILE = "sampled_5033779.parquet"
OUTPUT_FILE = "pred_5p5M_3.parquet"
MODEL_PATH = "./results/best_model"
MODEL_NAME = "uitnlp/visobert"

LABEL2ID = {
    "admiration": 0, "amusement": 1, "anger": 2, "annoyance": 3, "approval": 4,
    "caring": 5, "confusion": 6, "curiosity": 7, "desire": 8, "disappointment": 9,
    "disapproval": 10, "disgust": 11, "embarrassment": 12, "excitement": 13,
    "fear": 14, "gratitude": 15, "grief": 16, "joy": 17, "love": 18,
    "nervousness": 19, "optimism": 20, "pride": 21, "realization": 22,
    "relief": 23, "remorse": 24, "sadness": 25, "surprise": 26, "neutral": 27,
}
ID2LABEL = {id: label for label, id in LABEL2ID.items()}

# MODEL INITIALIZATION
print("Initializing model and tokenizer...")
device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

tokenizer = AutoTokenizer.from_pretrained(MODEL_NAME)
model = AutoModelForSequenceClassification.from_pretrained(
    MODEL_PATH, 
    torch_dtype=torch.float16 if torch.cuda.is_available() else torch.float32
).to(device)
model.eval()

# DATASET WRAPPER FOR PARALLEL PREPROCESSING
class InferenceDataset(Dataset):
    def __init__(self, texts):
        self.texts = texts

    def __len__(self):
        return len(self.texts)

    def __getitem__(self, idx):
        return ViTokenizer.tokenize(str(self.texts[idx]))

def batch_predict(series: pl.Series) -> pl.Series:
    texts = series.to_list()
    out_labels = []
    out_scores = []
    
    ds = InferenceDataset(texts)
    loader = DataLoader(
        ds, 
        batch_size=MINI_BATCH_SIZE, 
        num_workers=24, 
        pin_memory=True,
        prefetch_factor=3,
    )
    
    for batch_texts in tqdm(loader, desc="Predicting Chunk", leave=False):
        inputs = tokenizer(
            list(batch_texts),
            padding=True,
            truncation=True,
            max_length=256,
            return_tensors="pt",
            return_token_type_ids=False,
        ).to(device)
        
        with torch.no_grad():
            with autocast(): 
                outputs = model(**inputs)
                probs = F.softmax(outputs.logits, dim=-1)
                conf_scores, label_ids = torch.max(probs, dim=-1)

        conf_scores = conf_scores.half().cpu().numpy()
        label_ids = label_ids.cpu().numpy()

        for score, l_id in zip(conf_scores, label_ids):
            out_labels.append(ID2LABEL[l_id])
            out_scores.append(float(score))
            
    return pl.Series([{"labels": l, "confidence": s} for l, s in zip(out_labels, out_scores)])

# STREAMING PIPELINE
lazy_df = pl.scan_parquet(PREDICT_FILE)

if os.path.exists(OUTPUT_FILE):
    os.remove(OUTPUT_FILE)

streaming_query = (
    lazy_df.with_columns(
        [
            pl.col("text")
            .map_batches(batch_predict, return_dtype=pl.Struct([pl.Field("labels", pl.String), pl.Field("confidence", pl.Float64)]))
            .alias("raw_out")
        ]
    )
    .with_columns(
        [
            pl.col("raw_out").struct.field("labels").alias("labels"),
            pl.col("raw_out").struct.field("confidence").alias("confidence"),
        ]
    )
    .drop("raw_out")
)

# EXECUTION
print(f"Starting Prediction Engine on: {device.type.upper()}")
start_time = time.perf_counter()

streaming_query.sink_parquet(
    OUTPUT_FILE,
    row_group_size=CHUNK_SIZE,
)

end_time = time.perf_counter()
duration = end_time - start_time

print("-" * 30)
print(f"Processing Complete!")
print(f"Total Execution Time: {duration:.2f}s ({duration/60:.2f} mins)")
print(f"Output File: {OUTPUT_FILE}")
