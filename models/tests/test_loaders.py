import pytest
import os
import torch
from transformers import AutoTokenizer
from loaders import get_dataloader, get_label_converter, truncate_text


@pytest.fixture(scope="module")
def tokenizer(request):
    name = request.param
    tokenizer_map = {
        "m_bert": "google-bert/bert-base-multilingual-cased",
        "pho_bert": "vinai/phobert-base-v2",
        "m_modern_bert": "jhu-clsp/mmBERT-base",
    }
    return AutoTokenizer.from_pretrained(tokenizer_map.get(name))


@pytest.fixture
def label_mapping():
    return get_label_converter(shuffle=False)


def test_label_converter_logic(label_mapping):
    label2id, id2label = label_mapping

    assert len(label2id) == 28


def test_truncate_text():
    text_short = "Title [CHANNEL] Tech [COMMENT] Hello world [IN_YEAR] 2024"
    assert truncate_text(text_short, 512) == text_short

    long_title = " ".join(["adb"] * 150)
    long_comment = " ".join(["bsc"] * 400)
    long_reply = " ".join(["rks"] * 400)

    text_lr = f"[TITLE] {long_title} [CHANNEL] Tech [CATEGORY] howto [COMMENT] {long_comment} [REPLY] {long_reply} [IN_YEAR] 2024"
    text_lp = f"[TITLE] {long_title} [CHANNEL] Tech [CATEGORY] howto [COMMENT] {long_comment} [IN_YEAR] 2024"

    truncated_lr = truncate_text(text_lr, 512)
    truncated_lp = truncate_text(text_lp, 512)

    # Check [TITLE] [CHANNEL] and [CATEGORY] [COMMENT] [REPLY] [IN_YEAR] are preserved
    assert (
        "[TITLE]" in truncated_lr
        and "[CHANNEL]" in truncated_lr
        and "[CATEGORY]" in truncated_lr
        and "[COMMENT]" in truncated_lr
        and "[REPLY]" in truncated_lr
        and "[IN_YEAR]" in truncated_lr
    )
    assert (
        "[TITLE]" in truncated_lp
        and "[CHANNEL]" in truncated_lp
        and "[CATEGORY]" in truncated_lp
        and "[COMMENT]" in truncated_lp
        and "[IN_YEAR]" in truncated_lp
    )
    assert "[IN_YEAR] 2024" in truncated_lr and "[IN_YEAR] 2024" in truncated_lp
    assert (
        "[CHANNEL] Tech [CATEGORY] howto" in truncated_lr
        and "[CHANNEL] Tech [CATEGORY] howto" in truncated_lp
    )


@pytest.mark.parametrize(
    "tokenizer", ["m_bert", "pho_bert", "m_modern_bert"], indirect=True
)
def test_loader(tokenizer):
    train_loader = get_dataloader(
        data_root="data",
        tokenizer=tokenizer,
        max_len=512,
        batch_size=16,
        num_workers=4,
        shuffle=True,
        labels_shuffle=False,
        split="train",
        text_col="text",
        label_col="labels",
        use_vitokenizer=False,
    )

    for batch in train_loader:
        # check hf keys
        assert "input_ids" in batch
        assert "attention_mask" in batch
        assert "labels" in batch

        # check shape
        print("input_ids.shape ", batch["input_ids"].shape)
        print("attention_mask.shape ", batch["attention_mask"].shape)

        print(batch["input_ids"])
        print()
        print(batch["attention_mask"])
        print()
        print(batch["labels"])
        print()

        break
