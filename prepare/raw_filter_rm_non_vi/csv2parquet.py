import argparse
import polars as pl


def csv2parquet(input_path: str, output_path: str):
    """
    Chuyển đổi file CSV sang Parquet sử dụng cơ chế Lazy Load của Polars.
    """
    try:
        # Khởi tạo LazyFrame từ file CSV
        # Polars sẽ không load dữ liệu vào RAM ngay lập tức
        lazy_df = pl.scan_csv(input_path)

        # Thực thi ghi dữ liệu trực tiếp xuống file Parquet (Streaming)
        # sink_parquet giúp xử lý các file lớn hơn cả dung lượng RAM hiện có
        lazy_df.sink_parquet(
            output_path,
            compression="snappy",  # Hoặc 'zstd', 'lz4'
            row_group_size=None,  # Tự động tối ưu row group
        )

        print(f"✅ Đã chuyển đổi thành công: {output_path}")

    except Exception as e:
        print(f"❌ Có lỗi xảy ra: {e}")


# Ví dụ sử dụng:
# csv2parquet("data_khong_lo.csv", "data_toi_uu.parquet")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Chuyển đổi file CSV sang Parquet sử dụng Polars."
    )
    parser.add_argument(
        "--input_path", type=str, help="Đường dẫn đến file CSV đầu vào."
    )
    parser.add_argument(
        "--output_path", type=str, help="Đường dẫn đến file Parquet đầu ra."
    )

    args = parser.parse_args()

    csv2parquet(args.input_path, args.output_path)
