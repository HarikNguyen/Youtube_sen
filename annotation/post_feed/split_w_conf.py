import os
import polars as pl
import argparse

INPUT_EXT = "parquet"
THRESHOLD = 0.6
BATCH_SIZE = 500_000
ACCEPT_FOLDER = "accepted_pred"


def split_res_data(input_name, threshold=THRESHOLD, label="", unwrite=False, recheck=False):

    input_file = f"{input_name}.{INPUT_EXT}"
    pf = pl.scan_parquet(input_file)

    # if label in columns, change it to labels
    if "label" in pf.columns:
        pf = pf.rename({"label": "labels"})

    total = pf.select(pl.len()).collect().item()

    if len(label) > 0:
        pf = pf.filter(pl.col("labels") == label)

    # Get accepted
    accepted_query = pf.filter(pl.col("confidence") >= threshold).select(
        ["comment_id", "text", "labels"]
    )
    accepted_len = accepted_query.select(pl.len()).collect().item()
    if not unwrite:
        if os.path.exists(f"{ACCEPT_FOLDER}/{input_name}_accepted.parquet"):
            os.remove(f"{ACCEPT_FOLDER}/{input_name}_accepted.parquet")
        accepted_query.sink_parquet(
            f"{ACCEPT_FOLDER}/{input_name}_accepted.parquet",
            engine="streaming",
            row_group_size=BATCH_SIZE,
        )

    # Get unaccepted
    unaccepted_query = pf.filter(pl.col("confidence") < threshold).select(
        ["comment_id", "text", "labels", "confidence"]
    )
    unaccepted_len = unaccepted_query.select(pl.len()).collect().item()
    if not unwrite and recheck:
        if os.path.exists(f"{ACCEPT_FOLDER}/{input_name}_unaccepted.parquet"):
            os.remove(f"{ACCEPT_FOLDER}/{input_name}_unaccepted.parquet")
        unaccepted_query.sink_csv(
            f"{input_name}_unaccepted.csv", engine="streaming", batch_size=BATCH_SIZE
        )

    print(f"Accepted: {accepted_len}/{total} ({accepted_len/total:.2%})")
    print(f"Unaccepted: {unaccepted_len}/{total} ({unaccepted_len/total:.2%})")


def main():

    if not os.path.exists(ACCEPT_FOLDER):
        os.makedirs(ACCEPT_FOLDER)

    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--input",
        type=str,
        help=f"Input file name without extension (expects {INPUT_EXT} format)",
    )
    parser.add_argument(
        "--threshold",
        type=float,
        default=THRESHOLD,
        help="Confidence threshold for splitting data",
    )
    parser.add_argument(
        "--unwrite",
        action="store_true",
        help="Whether to write the split data to files (default: False)",
    )
    parser.add_argument(
        "--recheck",
        action="store_true",
        help="Whether to recheck the unaccepted data (default: False)",
    )
    parser.add_argument(
        "--label",
        type=str,
        default="",
        help="Label to filter data by (default: "")",
    )
    args = parser.parse_args()
    split_res_data(args.input, args.threshold, args.label, args.unwrite, args.recheck)


if __name__ == "__main__":
    main()
