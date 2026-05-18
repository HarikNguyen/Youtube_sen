import torch
import torch.nn as nn
from transformers import AutoTokenizer, AutoModel, AutoConfig, get_scheduler
from loaders import get_label_converter

__model__ = ["m_bert", "pho_bert", "m_modern_bert"]
__model_map__ = {
    "m_bert": "google-bert/bert-base-multilingual-cased",
    "pho_bert": "vinai/phobert-base-v2",
    "m_modern_bert": "jhu-clsp/mmBERT-base",
}

def is_correct_model(model_name):
    return model_name in __model__

def get_model_name(model_name):
    return __model_map__.get(model_name)


class SAClassifier(nn.Module):
    def __init__(self, model, n_classes=28, tokenizer_len=None, dropout=0.1):
        super(SAClassifier, self).__init__()
        if model not in __model__:
            raise ValueError(f"Model name must be one of {__model__}")
        self.model_name = __model_map__.get(model)

        if model == "m_modern_bert": # only for m_modern_bert
            self.config = AutoConfig.from_pretrained(
                self.model_name,
                attention_dropout=dropout,
                classifier_dropout=dropout,
                embedding_dropout=dropout,
                mlp_dropout=dropout,
            )
        else: # for m_bert and pho_bert
            self.config = AutoConfig.from_pretrained(
                self.model_name,
                hidden_dropout_prob=dropout,
                attention_probs_dropout_prob=dropout,
            )

        self.backbone = AutoModel.from_pretrained(
            self.model_name,
            config=self.config,
        )

        if tokenizer_len is not None:
            self.backbone.resize_token_embeddings(tokenizer_len)

        self.dropout = nn.Dropout(dropout)
        self.classifier = nn.Linear(self.config.hidden_size, n_classes)

    def forward(self, input_ids, attention_mask):
        outputs = self.backbone(input_ids=input_ids, attention_mask=attention_mask)
        pooled_output = outputs.last_hidden_state[:, 0, :]
        x = self.dropout(pooled_output)
        logits = self.classifier(x)
        return logits
