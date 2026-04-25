import csv
from pathlib import Path


def count_total_comments(root_path):
    total_comments = 0
    file_count = 0
    root = Path(root_path)

    if not root.exists():
        print(f"'{root_path}' not found.")
        return

    for file_path in root.rglob("comment.csv"):
        try:
            with open(file_path, mode="r", encoding="utf-8") as f:
                reader = csv.reader(f)
                header = next(reader, None)

                count = sum(1 for row in reader)
                total_comments += count
                file_count += 1

        except Exception as e:
            print(f"Error while reading {file_path}: {e}")

    print("-" * 30)
    print(f"Found: {file_count} file 'comment.csv'")
    print(f"With total comment: {total_comments}")
    print("-" * 30)


if __name__ == "__main__":
    path_to_data = "raw_data"
    count_total_comments(path_to_data)
