import pandas as pd
from pathlib import Path
import os


def aggregate_large_youtube_comments(root_dir, output_file, chunk_size=100000):
    """
    Aggregates large volumes of CSV data using stream processing to minimize RAM usage.
    """
    base_path = Path(root_dir)
    output_path = Path(output_file)

    # Remove output file if it already exists to start fresh
    if output_path.exists():
        os.remove(output_path)

    is_first_chunk = True
    processed_count = 0

    # Desired column order
    column_order = [
        "video_id",
        "comment_id",
        "parent_id",
        "is_reply",
        "author",
        "comment",
        "like_count",
        "time_text",
        "created_at",
    ]

    # Use rglob to find all comment.csv files
    for csv_path in base_path.rglob("comment.csv"):
        try:
            video_id = csv_path.parent.name

            # Read and process in chunks in case a single video has massive comments
            for chunk in pd.read_csv(csv_path, chunksize=chunk_size):
                # Add video_id at the beginning
                chunk.insert(0, "video_id", video_id)

                # Reorder columns to ensure consistency
                chunk = chunk[column_order]

                # Append to the master CSV file
                # Write header only for the first chunk of the first file
                chunk.to_csv(
                    output_file,
                    mode="a",
                    index=False,
                    header=is_first_chunk,
                    encoding="utf-8-sig",
                )

                is_first_chunk = False

            processed_count += 1
            if processed_count % 100 == 0:
                print(f"Processed {processed_count} videos...")

        except Exception as e:
            print(f"Error processing {csv_path}: {e}")

    print(f"\nAggregation complete. Final data saved to: {output_file}")


if __name__ == "__main__":
    DATA_DIRECTORY = "raw_data"
    FINAL_OUTPUT = "master_comments_dataset.csv"

    aggregate_large_youtube_comments(DATA_DIRECTORY, FINAL_OUTPUT)
