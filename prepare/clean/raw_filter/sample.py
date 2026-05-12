import os
import random
import regex
import argparse
import polars as pl

DEFAULT_ROOT_FILE = "raw_comments.parquet"
DEFAULT_NON_VI_TEMPFILE = "non_vi_comments.csv"
DEFAULT_OUT_FILE = "vi_la_examples.txt"

def clean_text_udf(s: pl.Series) -> pl.Series:
    """User Defined Function (UDF) to clean text by:
    - Removing icons/emojis
    - Removing repeated words (e.g., "vui vui vui" -> "vui")
    - Handling extra whitespace
    - Lowercasing (already done in Polars for efficiency)
    """
    cleaned = []
    for text in s.to_list():
        t = str(text)
        t = regex.sub(r"[^\p{L}\p{N}\p{P}\s]", "", t)
        t = regex.sub(r"\b(\w+)(?:\s+\1\b)+", r"\1", t, flags=regex.IGNORECASE)
        t = regex.sub(r"\s+", " ", t).strip()
        cleaned.append(t)

    return pl.Series(cleaned, dtype=pl.String)


def manual_select_slang_comments(potential_list: list) -> list:
    """Manually select comments.
    This function have 4 mode:
    1. all (ra/sa): Remove all or Select All
    2. index (ri <index>/si <index>): Remove or Select comment by index in the potential list.
    3. range (rr <start> <end>/sr <start> <end>): Remove or Select a range of comments by index in the potential list.
    4. list (rl <index1> <index2> ... <indexN>/sl <index1> <index2> ... <indexN>): Remove or Select multiple comments by index in the potential list.
    """
    try:
        val = input("Enter the number of comments to review (e.g., 20): ").strip()
        split_num = int(val) if val else 20
    except ValueError:
        split_num = 20

    final_selected = []
    total = len(potential_list)

    for i in range(0, total, split_num):
        batch = potential_list[i : i + split_num]
        current_len = len(batch)

        os.system("cls" if os.name == "nt" else "clear")
        print("\n" + "═" * 60)
        for idx, item in enumerate(batch):
            print(f"[{idx}] {item}")

        print("═" * 60)
        print(
            f"That shows comments from {i} to {min(total, i+split_num)} out of {total}."
        )
        print(
            "Commands <<< [sa/ra] [si/ri <idx>] [sr/rr <start> <end>] [sl/rl <i1> <i2>...] | [q] >>>"
        )
        cmd_raw = input("Enter command: ").strip().lower()
        if not cmd_raw:
            continue
        if cmd_raw == "q":
            break

        parts = cmd_raw.split()
        mode = parts[0]
        args = parts[1:]
        indices_to_keep = set()
        all_indices = set(range(current_len))

        try:
            # ALL mode
            if mode == "sa":
                indices_to_keep = all_indices
            elif mode == "ra":
                indices_to_keep = set()

            # Invidual mode
            elif mode == "si":
                indices_to_keep = {int(args[0])}
            elif mode == "ri":
                indices_to_keep = all_indices - {int(args[0])}

            # 3. Range mode
            elif mode == "sr":
                start, end = int(args[0]), int(args[1])
                indices_to_keep = set(range(start, end + 1))
            elif mode == "rr":
                start, end = int(args[0]), int(args[1])
                indices_to_keep = all_indices - set(range(start, end + 1))

            # 4. List mode
            elif mode == "sl":
                indices_to_keep = {int(x) for x in args}
            elif mode == "rl":
                indices_to_keep = all_indices - {int(x) for x in args}

            else:
                print("Command invalid, skip this batch.")
                continue

        except Exception as e:
            print(f"Error while processing command: {e}")
            continue

        for local_idx in sorted(list(indices_to_keep)):
            if 0 <= local_idx < current_len:
                final_selected.append(batch[local_idx])

    return final_selected


def main():

    parser = argparse.ArgumentParser(description="Filter non-Vietnamese comments.")
    parser.add_argument("--input", type=str, default=DEFAULT_ROOT_FILE)
    parser.add_argument("--non_vi_temp", type=str, default=DEFAULT_NON_VI_TEMPFILE)
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE)

    args = parser.parse_args()

    raw_lf = pl.scan_parquet(args.input)
    nonvi_temp = pl.scan_csv(args.non_vi_temp)
    nonvi_tem_ids = nonvi_temp.select("comment_id").collect(engine="streaming").to_series()
    vi_lf = raw_lf.filter(~pl.col("comment_id").is_in(nonvi_tem_ids))

    # Get 4k completely certain vi-comments
    vi_list = (
        vi_lf
        .head(4000)
        .select("comment")
        .with_columns(
            [
                pl.col("comment").map_batches(clean_text_udf),
                pl.lit("__label__vi").alias("label"),
            ]
        )
        .with_columns(text=(pl.col("label") + " " + pl.col("comment")))
        .select("text")
        .collect()
        .to_series()
        .to_list()
    )
    vi_list_confirmed = manual_select_slang_comments(vi_list)

    # Get 6k uncertain other latin in non_vi_temp
    other_latin_list = (
        nonvi_temp
        .head(6000)
        .select("comment")
        .with_columns(
            [
                pl.col("comment").map_batches(clean_text_udf),
                pl.lit("__label__other").alias("label"),
            ]
        )
        .with_columns(text=(pl.col("label") + " " + pl.col("comment")))
        .select("text")
        .collect(engine="streaming")
        .to_series()
        .to_list()
    )
    other_latin_list_confirmed = manual_select_slang_comments(other_latin_list)

    # Concatenate, shuffle, and save
    training_data = vi_list_confirmed + other_latin_list_confirmed
    random.shuffle(training_data)
    with open(args.output, "w") as f:
        f.write("\n".join(training_data))


if __name__ == "__main__":
    main()
