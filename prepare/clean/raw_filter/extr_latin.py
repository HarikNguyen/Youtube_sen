import argparse
import regex
import polars as pl

DEFAULT_IN_FILE = "non_vi_comments.csv"
DEFAULT_OUT_FILE = "latin_comments.csv"

# Pre-compile regex patterns for performance
RE_ALPHANUM = regex.compile(r"\p{L}|\p{N}")
RE_LATIN = regex.compile(r"\p{Script=Latin}")


def get_latin_stats(text: str | None) -> dict:
    """
    Analyzes text to determine if it belongs to the Latin group based on character ratio.
    """
    if not text or not text.strip():
        return {"group": "empty", "ratio": 0.0}

    # Extract alphanumeric characters only
    clean_chars = RE_ALPHANUM.findall(text)
    total_len = len(clean_chars)

    if total_len == 0:
        return {"group": "non_language", "ratio": 0.0}

    # Calculate Latin ratio
    latin_chars_count = len(RE_LATIN.findall("".join(clean_chars)))
    latin_ratio = (latin_chars_count / total_len) * 100

    group = "LATIN_GROUP" if latin_ratio > 70 else "NON_LATIN_GROUP"

    return {"group": group, "ratio": latin_ratio}


def process_comments(input_path: str, output_path: str):
    """
    Processes large CSV files using Polars streaming to filter Latin comments.
    """
    # Define the compute graph
    pipeline = (
        pl.scan_csv(input_path)
        .filter(pl.col("comment").is_not_null())
        .with_columns(
            pl.col("comment")
            .map_elements(
                get_latin_stats,
                return_dtype=pl.Struct(
                    [pl.Field("group", pl.String), pl.Field("ratio", pl.Float64)]
                ),
            )
            .alias("stats")
        )
        .unnest("stats")
        # Keep only Latin group
        .filter(pl.col("group") == "LATIN_GROUP")
        .select(["comment_id", "comment"])
    )

    # Execute using streaming mode to handle files larger than RAM
    pipeline.sink_csv(output_path)


def main():
    parser = argparse.ArgumentParser(description="Filter non-Vietnamese comments.")
    parser.add_argument("--input", type=str, default=DEFAULT_IN_FILE)
    parser.add_argument("--output", type=str, default=DEFAULT_OUT_FILE)

    args = parser.parse_args()

    try:
        process_comments(args.input, args.output)
        print("Filtering completed successfully.")
    except Exception as e:
        print(f"Error during execution: {e}")


if __name__ == "__main__":
    main()
