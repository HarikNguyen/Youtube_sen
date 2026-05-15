# Standard library
import pandas as pd
import polars as pl

# Torch
from torch.utils.data import IterableDataset

# Transformers
from datasets import load_dataset

# pyvi NLP toolkit (https://github.com/trungtv/pyvi)
from pyvi.ViTokenizer import tokenize as vi_tokenizer  # for phoBERT


class ViEmoDataset(IterableDataset):
    def __init__(
        self,
        data_root,
        label2id,
        split="train",
        text_col="text",
        label_col="labels",
        use_vitokenizer=False,
    ):
        """A Dataset for Sentiment Analysis and Emotion Classification of Vietnamese YouTube Comments."""
        super(ViEmoDataset).__init__()
        # Get Standard parameters
        self.text_col = text_col
        self.label_col = label_col
        self.label2id = label2id
        self.data_root = data_root
        self.split = split

        # Streaming load dataset from parquet path (local - like - data/*.parquet or huggingface hub dataset)
        self.dataset = load_dataset(
            "parquet",
            data_files={self.split: self.data_root + self.split + ".parquet"},
            split=self.split,
            streaming=True,
        )
        # Initialize tokenizer
        self.vitokenizer = vi_tokenizer if use_vitokenizer else None  # for phoBERT

        # Define custom tokens for special fields
        self.field_tokens = [
            "[TITLE]",
            "[CHANNEL]",
            "[CATEGORY]",
            "[COMMENT]",
            "[REPLY]",
            "[IN_YEAR]",
        ]

    def __iter__(self):
        """Yields tokenized text and labels as tensors."""
        for example in self.dataset:
            text = example[self.text_col]
            label = example[self.label_col]

            # Tokenize text
            if self.vitokenizer is not None:
                text = self.vitokenizer(text)

            yield {"text": text, "labels": self.label2id[label]}

    def shuffle(self, seed, buffer_size):
        """Shuffles the dataset with a buffer."""
        self.dataset = self.dataset.shuffle(seed=seed, buffer_size=buffer_size)
        return self

    def labels_stats(self):
        """Returns the distribution of labels in the dataset."""
        # Get label distribution
        stats_df = pd.read_csv(self.data_root + self.split + "_labels.csv")
        # Convert to dictionary
        stats_dict = dict(zip(stats_df["labels"], stats_df["count"]))
        return stats_dict

    def length(self):
        """Returns the length of the dataset."""
        return pl.scan_parquet(self.data_root + self.split + ".parquet").select(pl.len()).collect().item()
