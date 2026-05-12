- Cài các thư viện sau.

```
pip install polars \
    pandas \
    numpy \
    matplotlib \
    seaborn \
    tqdm \
    scikit-learn \
    argparse \
    datasketch \
    pyarrow \
    regex \
```

- Tải `glotlid_v1`.

```
wget https://huggingface.co/cis-lmu/glotlid/resolve/main/model.bin -O glotlid_v1.bin
```

- Copy toàn bộ các file ra cùng một thư mục rồi chạy lần lượt các file.

```
cp ./raw_filter/* ./
# Nếu máy yếu chọn lệnh này!
python extr_non_vi.py

# Nếu máy mạnh chọn lệnh này. (42cores 128GB RAM. Chỉ chọn 1 trong 2 lệnh!!!)
python extr_non_vi_multi_cores.py
```

```
# Tiếp tục chạy các lệnh này để hoàn thành raw_filter.
python extr_latin.py
python sample.py
python deep_extr_vi.py
python combine.py
```

```
# Tiếp tục chạy lệnh này để normalize.
python normalizer.py
```

```
# Tiếp tục chạy lệnh này để deduplicate.
cp ./deduplicator/* ./
python hashing.py
python combine_hash.py
python dedup.py
```

```
# Tiếp tục chạy lệnh này cho heuristic filter.
cp ./heuristic_filter/* ./
python filter_q3.py
python recorver_after_filter.py
```

- File kết quả mặc định sẽ là `final_prepared_comments.parquet`
