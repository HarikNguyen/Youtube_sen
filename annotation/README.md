# CHẠY!

## Chuẩn bị tập seed!

- Hãy vào `prepare`.
- Điều chỉnh các tham số `OLD_SAMPLE` và `SAMPLE_SIZE` trong file `sample.py`
```
# sampled files (All sampled file after each fine-tune loop like ft_seed_+uit.parquet, ft_90k.parquet, ... etc)
OLD_SAMPLE = ["ft_848k.parquet","ft_173k.parquet", "ft_seed_+uit.parquet", "ft_90k.parquet"]

# the size of sample in next fine-tune loop (like 10000, 20000, ... etc). If None, all data will be sampled
SAMPLE_SIZE = None
```

- Sau đó chạy `python sample.py` thu được file `sample_xxx.parquet` với `xxx` là số lượng `SAMPLE_SIZE`.

- Tiếp tục flat dữ liệu trong `sample_xxx.parquet` thành các câu có dạng.
```
- [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> [IN_YEAR] <year> (Nếu comment này parent comment)
- - [TITLE] <video title> [CHANNEL] <channel name> [CATEGORY] <category name> [COMMENT] <comment> [REPLY] <reply> [IN_YEAR] <year> (Nếu comment này reply cho parent comment)
```

- Đồng thời ta cũng làm tương tự với tham số `OLD_SAMPLE = []` và `SAMPLE_SIZE = None` để lấy toàn bộ tập data hiện có dưới dạng flat.
- Tiếp đó chạy `python sample_with_regex.py` trong thư mục `prepare_seed` để tạo file `minority_candidates_to_label.csv` (gồm các câu được dự gắn nhãn giả **hiếm** với các quy luật, nhằm bổ sung cho file `sample_xxx.parquet`

- Cũng đồng thời tạo với tham số `OLD_SAMPLE = ["sample_xxx.parquet"]` và `SAMPLE_SIZE = None` nhằm tạo file `sample_yyy.parquet` để cho quá trình feed (gán nhãn giả).

- Sau đó, dùng API của Google Studio AI để dự đoán nhãn cho file `sample_xxx.parquet` vừa rồi.
- Ta chạy `python gem.py` trong thư mục `gemini_ai`.

- Đồng thời cũng chạy `python gem_check.py` với cho file `minority_candidates_to_label.csv`.

- Lưu ý **cả hai file** `gem.py` và `gem_check.py` phải thay đổi các tham số `INPUT_FILE` và `OUTPUT_FILE`

- Nếu chưa đủ, ta sẽ phải chạy file `gem_gen.py` để sinh thêm dữ liệu giả cho tập seed.

## Fine-tuning và feed

- Khi đã có tập seed (đổi tên thành `ft_seed_+uit.parquet`) thì chạy `python finetuning.py` trong thư mục `bert`.

- Sau khi chạy xong thì ta chạy `python feed.py` (sau khi đổi `PREDICT_FILE`" và `OUTPUT_FILE` trong thư mục `bert` để dự đoán nhãn cho file `sample_yyy.parquet`

## Hậu kiểm và chuẩn bị vòng tiếp theo.

- Sau khi đã xong thì ta chạy `python split_w_conf.py --input <tên_file_kết_quả_sau_khi_feed<(đã loại đi phần đuôi .parquet) --threshold .5`. Ví dụ `python split_w_conf.py --input pred_5M --threshold .5` Để lấy toàn bộ dữ liệu có confidence lớn hơn `threshold` (thường là `0.5` trở lên)

- Hoặc chạy `python top_K_conf.py` (thay đổi biến `K_SAMPLES`) để lấy `K_SAMPLES` dữ liệu có confidence cao nhất từ mỗi nhãn.

- Sau khi chọn ra tập dữ liệu để tiếp tục fine-tuning vòng tiếp theo. Có thể chạy `python balanced_seed.py` để cân bằng số lượng mẫu của các lớp.

## Vòng cuối cùng.

- Chạy file `python final_select.py` để chọn ra tập dữ liệu cuối cùng với ngưỡng confidence linh hoạt.

- Chạy `python concat_all.py` để chọn ra hợp nhất với tập dữ liệu ban đầu `final_prepared_comments.parquet` để hoàn tất quá trình gán nhãn.
