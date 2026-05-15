import torch
import random
from torch.utils.data import DataLoader
from .datasets import ViEmoDataset

LABELS = [
    "amusement",
    "excitement",
    "joy",
    "love",
    "desire",
    "optimism",
    "caring",
    "pride",
    "admiration",
    "gratitude",
    "relief",
    "approval",
    "realization",
    "surprise",
    "curiosity",
    "confusion",
    "fear",
    "nervousness",
    "remorse",
    "embarrassment",
    "disappointment",
    "sadness",
    "grief",
    "disgust",
    "anger",
    "annoyance",
    "disapproval",
    "neutral",
]


def get_label_converter(shuffle=False, rd_state=58):
    labels = LABELS.copy()
    random.seed(rd_state)
    if shuffle:
        random.shuffle(labels)
    label2id = {label: idx for idx, label in enumerate(labels)}
    id2label = {idx: label for idx, label in enumerate(labels)}
    return label2id, id2label

class ViEmoCollator:
    def __init__(self, tokenizer, max_len=512):
        self.tokenizer = tokenizer
        self.max_len = max_len

    def __call__(self, batch):
        """Collate function to encoding."""
        # get texts and labels from batch
        texts = [item["text"] for item in batch]
        labels = [item["labels"] for item in batch]

        # truncate texts to fit max_len
        texts = [
            truncate_text(text, self.max_len - 2) for text in texts
        ]  # 2 for [CLS] and [SEP] tokens

        # encode texts with tokenizer
        encodings = self.tokenizer(
            texts,
            add_special_tokens=True, # add [CLS] and [SEP] tokens
            max_length=self.max_len,
            truncation=True,
            padding=True, # dynamically pad to the longest sequence in the batch
            return_tensors="pt",
        )
        # add labels as tensor
        encodings["labels"] = torch.tensor(labels, dtype=torch.long)
        return encodings


def get_dataloader(
    data_root, tokenizer, max_len=512, batch_size=16, num_workers=4, shuffle=True, shuffle_size=500_000, shuffle_seed=49, labels_shuffle=False, split="train", text_col="text", label_col="labels", use_vitokenizer=False, return_vocab_size=False, return_label_dict=False, return_label_stats=False, return_num_o_iter=False, return_num_o_set=False,
):
    """Returns a DataLoader for the ViEmoDataset."""

    # init label dictionaries
    label2id, id2label = get_label_converter(shuffle=labels_shuffle)

    # create dataset
    dataset = ViEmoDataset(
        data_root=data_root,
        label2id=label2id,
        split=split,
        text_col=text_col,
        label_col=label_col,
        use_vitokenizer=use_vitokenizer,
    )

    # torch dataloader with IterableDataset not support shuffle, so we will shuffle the dataset with batch level
    if shuffle:
        dataset = dataset.shuffle(seed=shuffle_seed, buffer_size=shuffle_size)

    # define a collate function to encode the text and labels
    tokenizer.add_tokens(dataset.field_tokens)  # add custom tokens to tokenizer

    # create dataloader
    collate_fn = ViEmoCollator(tokenizer, max_len=max_len)
    dataloader = DataLoader(
        dataset,
        batch_size=batch_size,
        num_workers=num_workers,
        collate_fn=collate_fn,
        shuffle=False,  # must set to False!!!
    )

    # returns
    returns = [dataloader]
    if return_vocab_size:
        returns.append(len(tokenizer))
    if return_label_dict:
        returns.append((label2id, id2label))
    if return_label_stats:
        label_stats = dataset.labels_stats()
        returns.append(label_stats)
    if return_num_o_iter:
        num_o_iter = dataset.length()//batch_size + 1
        returns.append(num_o_iter)
    if return_num_o_set:
        returns.append(dataset.length())

    # final return
    if len(returns) == 1:
        return returns[0]
    else:
        return tuple(returns)


def truncate_text(text, max_len):
    """Truncate text to max_len tokens.
    If text is longer than max_len (ex.512), we will keep 100 tokens for metadata, and device the rest into 3 parts: parent_comment, reply_comment (if exist) and year. parent_comment and reply_comment will be truncated to (max_len - 2)//2 tokens each.
    If text is shorter than max_len, we will keep it as is.
    """
    words = text.split()

    if len(words) <= max_len:
        return text

    # split text into metadata and content
    meta_part, content_full = text.split("[COMMENT]", 1)
    title, channel_n_cat = meta_part.split("[CHANNEL]", 1)  # keep channel and category
    content_part, year = content_full.rsplit("[IN_YEAR]", 1)  # keep year

    # split parent and child comment if exist (by [REPLY])
    if "[REPLY]" in content_part:
        parent, reply = content_part.split("[REPLY]", 1)
        p_len = r_len = (
            max_len - 103
        ) // 2  # 103 = 100 for metadata + 2 for [IN_YEAR] <year> + 1 for REPLY
    else:
        parent, reply = content_part, ""
        p_len = max_len - 102  # 102 = 100 for metadata + 2 for [IN_YEAR] <year>
        r_len = 0

    # truncate!
    title_limit = (
        100 - len(channel_n_cat.split()) - 2
    )  # 1 for [CHANNEL] token and 1 for [COMMENT]
    title_truncated = " ".join(title.split()[:title_limit])
    parent_truncated = " ".join(parent.split()[:p_len])
    reply_truncated = " ".join(reply.split()[:r_len])

    # union!
    metadata_truncated = f"{title_truncated} [CHANNEL] {channel_n_cat.strip()}"
    if len(reply_truncated) > 0:
        text_truncated = (
            metadata_truncated
            + " [COMMENT] "
            + parent_truncated
            + " [REPLY] "
            + reply_truncated
            + " [IN_YEAR] "
            + year.strip()
        )
    else:
        text_truncated = (
            metadata_truncated
            + " [COMMENT] "
            + parent_truncated
            + " [IN_YEAR] "
            + year.strip()
        )

    return text_truncated
