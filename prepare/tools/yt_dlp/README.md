## Hướng dẫn sử dụng

1. `craw_comments.py`: Thu thập comments, metadata, transcript từ danh sách video sau khi được thu thập từ scrapetube và cập nhật số lượng comment từ api (`video_stats_final_v2.csv`).

2. `agg_comments.py`: Tổng hợp tất cả raw comments thành một file duy nhất.

3. `count_cmt.py`: Đếm số lượng comment trong toàn bộ raw\_data.

4. `expand_seed.py`: Mở rộng tập seed thay thế cho api vì không có giới hạn quotas.
