import argparse
import polars as pl
import numpy as np
from sklearn.model_selection import train_test_split
import os

DEFAULT_INPUT = "unsampled_comments.parquet"
DEFAULT_OUTPUT_DIR = "."
DEFALUT_VAL_RATE = 0.05
DEFALUT_TEST_RATE = 0.05
DEFALUT_RANDOM_STATE = 54
BATCH_SIZE = 500_000


def filter_and_sink(lf, indices, out_dir, filename):
    print(f"Filtering {filename}...")
    idx_lf = pl.DataFrame({"row_nr": indices.astype(np.uint32)}).lazy()

    print(f"Saving {filename}...")
    (
        lf.join(idx_lf, on="row_nr", how="semi")
        .drop("row_nr")
        .sink_parquet(
            os.path.join(out_dir, filename),
            row_group_size=BATCH_SIZE,
        )
    )
    print(f"Saved {filename} with {len(indices)} rows.")


def split_w_rate(
    in_lf,
    out_dir,
    val_rate=DEFALUT_VAL_RATE,
    test_rate=DEFALUT_TEST_RATE,
    random_state=DEFALUT_RANDOM_STATE,
):

    # Get labels list as numpy array to calc the ratios
    labels = in_lf.select("labels").collect().to_series().to_numpy()
    indices = np.arange(len(labels))

    # Split train and temp (val + test) sets
    print("Splitting training set...")
    train_indices, temp_indices = train_test_split(
        indices,
        test_size=val_rate + test_rate,
        random_state=random_state,
        stratify=labels,
    )

    # Split temp into validation and test sets
    print("Splitting validation and test sets...")
    val_indices, test_indices = train_test_split(
        temp_indices,
        test_size=test_rate / (val_rate + test_rate),
        random_state=random_state,
        stratify=labels[temp_indices],
    )

    # Convert back to Polars DataFrames
    filter_and_sink(in_lf, train_indices, out_dir, "train.parquet")
    filter_and_sink(in_lf, val_indices, out_dir, "val.parquet")
    filter_and_sink(in_lf, test_indices, out_dir, "test.parquet")


def main():
    parser = argparse.ArgumentParser(
        description="Split dataset into train, val, and test sets."
    )
    parser.add_argument(
        "--input_file",
        type=str,
        default=DEFAULT_INPUT,
        help="Path to the input Parquet file.",
    )
    parser.add_argument(
        "--output_dir",
        type=str,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to save the split datasets.",
    )
    parser.add_argument(
        "--val_rate",
        type=float,
        default=DEFALUT_VAL_RATE,
        help="Proportion of the dataset to include in the validation set.",
    )
    parser.add_argument(
        "--test_rate",
        type=float,
        default=DEFALUT_TEST_RATE,
        help="Proportion of the dataset to include in the test set.",
    )
    parser.add_argument(
        "--random_state",
        type=int,
        default=DEFALUT_RANDOM_STATE,
        help="Random seed for reproducibility.",
    )

    args = parser.parse_args()

    output_dir = args.output_dir
    os.makedirs(output_dir, exist_ok=True)

    print(f"Scaning input file: {args.input_file}...")
    in_lf = pl.scan_parquet(args.input_file).with_row_index("row_nr")

    split_w_rate(
        in_lf,
        out_dir=args.output_dir,
        val_rate=args.val_rate,
        test_rate=args.test_rate,
        random_state=args.random_state,
    )


if __name__ == "__main__":
    main()
