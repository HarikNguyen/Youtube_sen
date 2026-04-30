import polars as pl
import regex
import unicodedata
import html

INPUT_FILE = "raw_filtered_comments.parquet"
OUTPUT_FILE = "norm_filtered_comments.parquet"
BATCH_SIZE = 1_000_000

# Compile regex outside the function for performance optimization
RE_ICON_REPEAT = regex.compile(r"((?!\d)(?=\p{Emoji})\X)\1{2,}")
RE_ICON_CLUSTER = regex.compile(r"((?:(?!\d)(?=\p{Emoji})\X|[\p{Zs}\t])+?)\1+")
RE_BRACKET_ELONG = regex.compile(r"([()\[\]{}])\1{2,}")
RE_SIGN_ELONG = regex.compile(r"([<>=:;-])\1{2,}")
RE_CHAR_ELONG = regex.compile(r"(\p{L})\1{2,}")
RE_TEXT_CLUSTER = regex.compile(r"(.{2,}?)\1{2,}")

############################################################################################
##                                 Helper Functions (Scalar Level)                        ##
############################################################################################


def _clean_text_scalar(text: str | None) -> str | None:
    """Handles complex Python-based logic on individual text units."""
    if text is None:
        return None

    # 1. Decode HTML & Normalize Unicode
    text = unicodedata.normalize("NFKC", html.unescape(text))

    # 2. Handle repeating icons (Keep maximum of 2 identical consecutive icons)
    text = RE_ICON_REPEAT.sub(r"\1\1", text)

    # 3. Handle repeating icons at cluster level
    prev_text = None
    while text != prev_text:
        prev_text = text
        text = RE_ICON_CLUSTER.sub(r"\1", text)

    return text


def _trim_all_elongations(text: str) -> str:
    """Python function to handle and trim all elongated character repetitions."""
    if not text:
        return text

    # Limit consecutive brackets/parentheses to a maximum of 3
    # (e.g., "(((((" -> "(((", ":))))" -> ":)))")
    text = RE_BRACKET_ELONG.sub(r"\1\1\1", text)

    # Limit consecutive signs to a maximum of 2
    # (e.g., ">>>>>>" -> ">>", "=====D" -> "==D")
    text = RE_SIGN_ELONG.sub(r"\1\1", text)

    # Limit consecutive letters to a maximum of 3
    # (e.g., "sooooo" -> "sooo", "greatttttt" -> "greattt")
    text = RE_CHAR_ELONG.sub(r"\1\1\1", text)

    # Limit word repetitions to a maximum of 3
    # (e.g., "hhahahaha" -> "hhahaha", "hahaha" -> "hahaha")
    text = RE_TEXT_CLUSTER.sub(r"\1\1\1", text)

    return text


############################################################################################
##                               Pipeline Steps (Step Level)                              ##
############################################################################################


def step_pre_process(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Step 1: Process HTML, Unicode, and repeating icons."""
    return lf.with_columns(
        [pl.col("comment").map_elements(_clean_text_scalar, return_dtype=pl.String)]
    )


def step_masking(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Step 2: Mask sensitive information and junk characters."""
    return lf.with_columns(
        [
            pl.col("comment")
            .str.replace_all(r"[\p{Cf}]", "")
            .str.replace_all(r"[\p{Cc}\p{Zl}\p{Zp}]", "\n")
            .str.replace_all(r"(?i)https?://\S+|www\.\S+", "[URL]")
            .str.replace_all(r"\S+@\S+", "[EMAIL]")
            .str.replace_all(r"\b\d{9,11}\b", "[PHONE]")
            .str.replace_all(r"\d{12,}", "[LONG_NUMBER]")
            .str.replace_all(r"@\S+", "[USER]")
            .str.replace_all(r"\r", "")
            .str.replace_all(r"\t", " ")
        ]
    )


def step_normalize_structure(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Step 3: Normalize sentence structure, line breaks, and punctuation."""
    return lf.with_columns(
        [
            pl.col("comment")
            .str.replace_all(r"[ \t]*\n+[ \t]*", "\n")
            # Handle newlines
            .str.replace_all(r"([.!?])\n+", "${1} ")
            .str.replace_all(r"\n+", "\n")
            # Reduce repetitive punctuation
            .str.replace_all(r"\!{2,}", "!!!")
            .str.replace_all(r"\?{2,}", "???")
            .str.replace_all(r"\.{2,}", "...")
            # Handle extra whitespace
            .str.replace_all(r"\s+", " ")
            .str.replace_all(r"\s+([.!?,])", "${1}")
            .str.strip_chars()
        ]
    )


def step_normalize_word(lf: pl.LazyFrame) -> pl.LazyFrame:
    """Step 4: Normalize each word."""
    return lf.with_columns(
        [
            pl.col("comment")
            # Opening and closing parenthesis must not have space inside
            .str.replace_all(r"\(\s+", "(").str.replace_all(r"\s+\)", ")")
            # Shorten tail repeat
            .map_elements(_trim_all_elongations, return_dtype=pl.String)
        ]
    )


############################################################################################
##                                        Main Flow                                       ##
############################################################################################


def main():
    # 1. Initialize LazyFrame from source file
    lf = pl.scan_parquet(INPUT_FILE).filter(pl.col("comment").is_not_null())

    # 2. Execute pipeline using .pipe() for cleaner debugging and modularity
    processed_lf = (
        lf.pipe(step_pre_process)
        .pipe(step_masking)
        .pipe(step_normalize_structure)
        .pipe(step_normalize_word)
        .filter(pl.col("comment").is_not_null())
    )

    # 3. Write data using Streaming (sink_parquet) to prevent OOM (Out of Memory)
    print("Executing processing and writing file...")
    processed_lf.sink_parquet(OUTPUT_FILE, row_group_size=BATCH_SIZE)
    print(f"Results saved to {OUTPUT_FILE}")


if __name__ == "__main__":
    main()
