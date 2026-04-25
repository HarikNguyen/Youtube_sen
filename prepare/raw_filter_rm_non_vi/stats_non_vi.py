import polars as pl
import os

# 1. Đọc file csv của bạn
df = pl.read_csv("non_vi_lang.csv")

# 2. Tạo thư mục để chứa các file output (tùy chọn)
output_dir = "split_files"
os.makedirs(output_dir, exist_ok=True)

# 3. Lấy danh sách các giá trị duy nhất trong cột 'lang_1'
# và lặp qua từng giá trị để xuất file
for lang, group in df.group_by("lang_1"):
    # Tạo tên file dựa trên mã ngôn ngữ
    file_name = f"{lang[0]}.csv"
    file_path = os.path.join(output_dir, file_name)

    # Ghi file
    group.write_csv(file_path)
    print(f"Đã xuất file: {file_path}")
