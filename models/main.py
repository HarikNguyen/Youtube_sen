import argparse
import yaml
import os
from types import SimpleNamespace

from scripts import run_train, run_test

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        config_dict = yaml.safe_load(f)

    config = {
        **config_dict['data'],
        **config_dict['model'],
        **config_dict['train'],
        **config_dict['loss'],
        "project_name": config_dict['project_name'],
        "mode": config_dict['mode']
    }
    return SimpleNamespace(**config)

def main():
    parser = argparse.ArgumentParser(description="ViEmo SA Model")
    parser.add_argument("--config", type=str, default="configs/base_config.yaml", help="Path to config file")

    parser.add_argument("--mode", type=str, help="Override mode (train/test)")
    parser.add_argument("--batch_size", type=int, help="Override batch size")
    parser.add_argument("--lr", type=float, help="Override learning rate")

    cli_args = parser.parse_args()

    # load YAML config
    args = load_config(cli_args.config)

    # override args
    if cli_args.mode: args.mode = cli_args.mode
    if cli_args.batch_size: args.batch_size = cli_args.batch_size
    if cli_args.lr: args.lr = cli_args.lr

    # set model
    args.model = args.type

    print(f"Staring {args.mode.upper()} mode for {args.project_name}")
    print(f"Model: [{args.model}] | Batch Size: [{args.batch_size}] | Learning Rate: [{args.lr}]")

    if args.mode == "train":
        run_train(args)
    elif args.mode == "test":
        run_test(args)
    else:
        raise ValueError(f"Mode must be one of {'train', 'test'}")

    print(f"Finished {args.mode.upper()} mode for {args.project_name}")

if __name__ == "__main__":
    main()
