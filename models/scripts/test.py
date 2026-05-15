import os
import json
import torch
import torch.optim as optim
from torch.cuda.amp import autocast
from transformers import AutoTokenizer
from tqdm import tqdm

from models import SAClassifier, is_correct_model, get_model_name
from loaders import get_dataloader
from utils import GHMC_Loss, get_cb_weights, compute_metrics

def run_test(args):
    # set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # set model_name
    if not is_correct_model(args.model):
        raise ValueError(f"Model name must be one of {__model__}")
    model_name = get_model_name(args.model)

    # get tokenizer (pretrained)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    # define test_loader and important values
    test_loader, vocab_size, label_dict, label_stats, num_o_testiter  = get_dataloader(
        data_root=args.data_root,
        tokenizer=tokenizer,
        max_len=args.max_len,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        shuffle=True,
        shuffle_size=args.shuffle_size,
        shuffle_seed=args.shuffle_seed,
        labels_shuffle=False,
        split="test",
        text_col=args.text_col,
        label_col=args.label_col,
        use_vitokenizer=True if args.model == "pho_bert" else False,
        return_vocab_size=True,
        return_label_dict=True,
        return_label_stats=True,
        return_num_o_iter=True,
    )
    label2id, id2label = label_dict

    # define model
    model = SAClassifier(args.model, n_classes=args.n_classes, tokenizer_len=vocab_size)

    checkpoint_path = os.path.join(checkpoint_dir, f"{args.model}_best.pth")
    if not os.path.exists(checkpoint_path):
        raise ValueError(f"Checkpoint not found at {checkpoint_path}")

    # load checkpoint
    checkpoint = torch.load(checkpoint_path)
    model.load_state_dict(checkpoint["model_state_dict"])
    model.to(device)

    # evaluate
    model.eval()
    
    # define loss
    cb_weights = get_cb_weights(label_stats, label2id).to(device)
    criterion = GHMC_Loss(bins=args.ghm_bins, cb_weights=cb_weights)

    # evaluating
    all_preds = []
    all_labels = []
    test_loss = 0

    with torch.no_grad():
        pbar = tqdm(test_loader, desc="Testing {args.model}", total=num_o_testiter)
        for batch in pbar:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            with autocast(enabled=args.fp16):
                logits = model(input_ids, attention_mask)
                loss = criterion(logits, labels)
            test_loss += loss.item()

            preds = torch.argmax(logits, dim=1).cpu().numpy()
            all_preds.extend(preds)
            all_labels.extend(labels.cpu().numpy())

    # compute metrics
    avg_loss = test_loss / len(test_loader)
    metrics = compute_metrics(all_labels, all_preds, id2label)

    # print metrics
    print("\n"+"-"*50)
    print("Test results")
    print(f"> Test loss: {avg_loss:.4f}")
    print(f"> F1 macro: {metrics[0]:.4f}")
    print(f"> Avg acc: {metrics[1]:.4f}")
    print(f"> Balanced acc: {metrics[2]:.4f}")
    print(f"> Acc per label: {metrics[3]}")
    print(f"> Report:\n{metrics[4]}")
    print("-"*50)
