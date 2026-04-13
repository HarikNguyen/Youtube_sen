## Hướng dẫn sử dụng.


Tạo file `.env` trong thư mục gốc của dự án và thêm vào nội dung **tương tự** như sau:

```bash
API_KEY=AIzaSXAC94nDGLXP7s-cyCKc0d54fAoxa-tDgMg
HTTP_PROXY=http://reagan61616:ntu4odqwmtmx@103.216.74.218:4454
HTTPS_PROXY=http://reagan61616:ntu4odqwmtmx@103.216.74.218:4454
```

Và tương tự cho file `.api` với nội dung:

```bash
AIzamyAvKlqsmevMTjbDpuyaJQnYBJX0hspM3KY
AIzaSyaPrO3DUc6pJmnOJx0qVcHLYfVwzYRhczg
AIzaSyAKCfDK2wsrhik2FCNNdprvm6OwpclieIw
AIzaSyAeghaDxxLUoRrYM0bbnOEy1d4VEizrE48
AIzaSyAC95nDGLXP7s-cyCfc0d54fAoxa-tDgMg
AIzaSyDHmqIKXDYmjApTMsxIk1dGE7fhm6sLVAI
AIzaSyCWf4xYAvPOguoPd4OwPnGg9tkO-MDgEeQ
AIzaSyCMO2ejaaRh9zAVrd_xT0G4ZkwQrYs2ng
AIzaSyBRn9jc8o4VKMrWUw8YTp3i_9E79PrkW6g
AIzaSyBBWvt9RghMLEd0UdJwMM9tiTK2Aw3_w6g
```

Lưu ý, bạn phải có tập seed (`seed.json`) là danh sách các video ID mà bạn muốn mở rộng, sau đó thu thập.

Các lệnh cơ bản:

```python
python main.py --create_seed
```

Dùng để tạo tập seed thông qua API nếu bạn muốn làm vậy.

```python
python main.py --expanded_seed
```

Dùng để mở rộng tập seed theo cơ chế đề xuất của YouTube, và nó sẽ mở rộng cứ mỗi video thì sẽ lấy ba kênh gần nhất, cho đến khi đạt được số lượng kênh nhất định (mặc định là 10000), hoặc khi không mở rộng được nữa.

```python
python main.py --crawl
```

Dùng để bắt đầu quá trình thu thập dữ liệu, nó sẽ thu thập thông tin và bình luận kể cả transcript của các video trong tập seed, tuy nhiên, việc này sẽ rất tốn quota do đó, chúng ta sẽ thực hiện nó qua công cụ khác.

```python
python main.py --track_info
```

Dùng để thêm số lượng thống kê bình luận của video khi đã crawl (khi dùng công cụ khác).

