import os
import torch
import torch.optim as optim
from torch.amp import GradScaler, autocast
from transformers import AutoTokenizer
from tqdm import tqdm

from models import SAClassifier, is_correct_model, get_model_name
from loaders import get_dataloader
from utils import GHMC_Loss, get_cb_weights, compute_metrics, EarlyStopping


def run_train(args):
    # set device
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # set model_name
    if not is_correct_model(args.model):
        raise ValueError(f"Model name must be one of {__model__}")
    model_name = get_model_name(args.model)

    # get tokenizer (pretrained)
    tokenizer = AutoTokenizer.from_pretrained(model_name)

    # define train_loader and important values
    train_loader, vocab_size, label_dict, label_stats, num_o_trainiter = get_dataloader(
        data_root=args.data_root,
        tokenizer=tokenizer,
        max_len=args.max_len,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        streaming=args.streaming,
        shuffle=True,
        shuffle_size=args.shuffle_size,
        shuffle_seed=args.shuffle_seed,
        labels_shuffle=False,
        split="train",
        text_col=args.text_col,
        label_col=args.label_col,
        use_vitokenizer=True if args.model == "pho_bert" else False,
        return_vocab_size=True,
        return_label_dict=True,
        return_label_stats=True,
        return_num_o_iter=True,
    )
    label2id, id2label = label_dict

    # define val_loader
    val_loader, num_o_valiter = get_dataloader(
        data_root=args.data_root,
        tokenizer=tokenizer,
        max_len=args.max_len,
        batch_size=args.batch_size,
        num_workers=args.num_workers,
        streaming=args.streaming,
        shuffle=False,
        shuffle_size=args.shuffle_size,
        shuffle_seed=args.shuffle_seed,
        labels_shuffle=False,
        split="val",
        text_col=args.text_col,
        label_col=args.label_col,
        use_vitokenizer=True if args.model == "pho_bert" else False,
        return_num_o_iter=True,
    )

    # define model
    model = SAClassifier(args.model, n_classes=args.n_classes, tokenizer_len=vocab_size, dropout=args.dropout)
    model.to(device)

    # define loss
    cb_weights = get_cb_weights(label_stats, label2id).to(device)
    criterion = GHMC_Loss(bins=args.ghm_bins, cb_weights=cb_weights)

    # define optim
    accumulate_steps = getattr(
        args, "accumulate_steps", 1
    )  # for gradient accumulation (batch_size_truth = batch_size * accumulate_steps)
    scaler = GradScaler("cuda", enabled=args.fp16)
    optimizer = optim.AdamW(model.parameters(), lr=args.lr)

    # train loop
    best_f1 = 0.0
    checkpoint_dir = args.checkpoint_dir
    if not os.path.exists(checkpoint_dir):
        os.makedirs(checkpoint_dir, exist_ok=True)
    early_stopping = EarlyStopping(patience=3)

    for epoch in range(args.epochs):
        model.train()
        train_loss = 0.0
        pbar = tqdm(
            enumerate(train_loader),
            desc=f"Training {args.model} || Ep {epoch+1}/{args.epochs}",
            total=num_o_trainiter,
        )

        # training stage
        for bid, batch in pbar:
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"]
            att_shape = attention_mask.shape
            attention_mask = attention_mask.to(device)
            labels = batch["labels"].to(device)

            # forward using autocast fp16
            with autocast("cuda", enabled=args.fp16):
                logits = model(input_ids, attention_mask)
                loss = criterion(logits, labels)
                loss = loss / accumulate_steps

            # backward (accumulate gradient)
            # scale loss and backward
            scaler.scale(loss).backward()

            # only update after accumulate steps
            if (bid + 1) % accumulate_steps == 0 or (bid + 1) == num_o_trainiter:
                # unscale and step optimize
                scaler.step(optimizer)
                scaler.update()
                optimizer.zero_grad()  # reset gradient after step

            train_loss += loss.item() * accumulate_steps
            print_postfix = {"loss": loss.item() * accumulate_steps}
            if args.debug:
                print_postfix["shape"] = att_shape
            pbar.set_postfix(print_postfix)

        # validating stage
        avg_train_loss = train_loss / num_o_trainiter
        avg_val_loss, val_f1, balanced_acc = validate(
            model, val_loader, num_o_valiter, criterion, device, id2label, args.fp16
        )
        print(f"Epoch {epoch+1} Summary: ", end="")
        print(f" | Train Loss: {avg_train_loss:.4f}", end="")
        print(f" | Val Loss:   {avg_val_loss:.4f}", end="")
        print(f" | Val F1 (Macro): {val_f1:.4f}", end="")
        print(f" | Val Balanced Acc: {balanced_acc:.4f}")

        if val_f1 > best_f1:
            best_f1 = val_f1
            model_path = os.path.join(checkpoint_dir, f"{args.model}_best.pth")

            torch.save(
                {
                    "epoch": epoch,
                    "model_state_dict": model.state_dict(),
                    "optimizer_state_dict": optimizer.state_dict(),
                    "f1_mac": val_f1,
                },
                model_path,
            )

        # check early stopping
        early_stopping(val_f1)
        if early_stopping.early_stop:
            print("Early stopping triggered.")
            break  # Stop training

    print(f"Best Val F1 (Macro): {best_f1:.4f}")


def validate(model, val_loader, num_o_valiter, criterion, device, id2label, fp16=False):
    model.eval()
    val_loss = 0
    all_preds = []
    all_labels = []

    with torch.no_grad():
        for batch in tqdm(
            val_loader, desc="Validating...", leave=False, total=num_o_valiter
        ):
            input_ids = batch["input_ids"].to(device)
            attention_mask = batch["attention_mask"].to(device)
            labels = batch["labels"].to(device)

            with autocast("cuda", enabled=fp16):
                outputs = model(input_ids, attention_mask)
                loss = criterion(outputs, labels)
            val_loss += loss.item()

            preds = outputs.argmax(dim=1)
            all_preds.extend(preds.cpu().numpy())
            all_labels.extend(labels.cpu().numpy())

    val_loss /= num_o_valiter
    f1_macro, avg_acc, balanced_acc, label_acc_dict, report = compute_metrics(
        all_labels, all_preds, id2label
    )
    return val_loss, f1_macro, balanced_acc
