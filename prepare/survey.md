# Khảo sát các bộ benchmark.

## Các bộ benchmark Việt Nam hiện tại!

- **ViLexNorm (Vietnamese Lexical Normalization - Tiếng Việt được chuẩn hóa theo từ vựng**. Tuy thu thập từ Facebook và Tiktok, nhưng ta có thể biết một số chi tiết sau.
    - Số lượng: 10467 cặp (~10k).
    - Có thể học hỏi cách họ chuẩn hóa từ vựng tiếng việt, như thế nào. Điều này nằm ở bước **preprocessing**.!
- **UIT-VSMEC (Vietnamese Social Media Emotion Corpus)** (2019).
    - Số lượng: ~7k câu.
    - Số lớp: 6 lớp cơ bản của Ekman (buồn bã, thích thú, tức giận, ghê tởm, sợ hãi, và bất ngờ).
    - Phương pháp gán nhãn thủ công.
- **ViGoEmotions** (Được công bố rất gần đây (xuất bản tại hội nghị EACL 2026)).
    - Số lượng: ~21k câu.
    - Số lớp: 28 lớp cảm xúc (vi phân cảm xúc thành 27 + 1 lớp trung tính) (được gán nhãn tự động).
    - Đa nhãn (multi-label) - một câu có nhiều cảm xúc.
    - Phương pháp gán nhãn tự động dựa trên mô hình học sâu.
    - Bộ dữ liệu đã kết hợp với bộ UIT-VSMEC.

**Ở trên là ba bộ benchmark** chính mà có liên quan đến việc phân tích sắc thái cảm xúc trong tiếng Việt. Như đã liệt kê, ta có thể nhận thấy một số điểm cần lưu ý sau, nhất là bộ mới vừa phát hành ViGoEmotions.
- Các bình luận luôn có các sắc thái cảm xúc khác nhau, không thể quy vào một chiều hướng lên (tích cực) hay xuống (tiêu cực) đơn nhất.
- Có thể nhận ra rằng, chiều hướng xây dựng bộ dữ liệu hiện tại luôn có bóng dáng của LLMs để gán nhãn tự động! Dễ thấy là cùng một công việc nhưng từ 7k lên tới 21k (tăng gấp 3 lần) nhờ vào việc gán nhãn tự động.

## Một vài nhận định!
- YouTube nói riêng hay MXH nói riêng, có một điều cần phải thừa nhận, các comment luôn có số lượng chênh lệch rất lớn, tức là bị bias. Cụ thể hơn là các cảm xúc tích cực luôn lấn át cảm xúc tiêu cực. Có thể lấy các tập ở trên để mà minh chứng cho điều này, đồng thời đây cũng là một hiện tượng tâm lý của con người - nguyên lý Pollyanna (Peter Sheridan Dodds - Human language reveals a universal positivity bias). Thậm chí còn tệ hơn khi mà YouTube sẽ ưu tiên hiển thị các comment tích cực hơn là tiêu cực, nghiêm trọng hơn là họ xóa chúng!
- YouTube là nền tảng chia sẻ nội dung số, do đó, có rất nhiều content và lĩnh vực khác nhau được đăng tải lên đó. Và dĩ nhiên, YouTube không bao giờ đưa ra một danh sách đầy đủ, họ chỉ đưa một kết quả gợi ý thông qua một hệ thống recommendation system.
- Một số nghiên cứu, cho thấy emoji không cần phải chỉnh sửa vì kiến trúc Transformer có thể học nó tốt! 
- Ngoài ra, một lưu ý cực kỳ quan trọng là các bộ benchmark hầu như rất ít mẫu. Thậm chí so với thế giới thì còn kém xa. Và để thực hiện xây dựng một khung phương pháp có thể chạy bài toán giải quyết trên "đa miền - multidomain" thì ta phải hướng tới việc mô hình cần có các đặc điểm sau.
    - Được huấn luyện trên lượng dữ liệu đủ lớn (lớn ở đây không có nghĩa là phải hàng triệu mẫu như các bộ benchmark quốc tế, nhưng phải đủ **đa dạng** để thích nghi với các domain khác nhau.
    - Không bị thiên kiến về một hướng cảm xúc nào, **không bias**.

## Kế hoạch sẽ thực hiện!

Mục tiêu, đặt ra thu thập ít nhất 500k mẫu dữ liệu, trên các lĩnh vực khác nhau, nếu được thì có thể 'càng quét' toàn bộ YouTube.

Dựa trên các nghiên cứu quốc tế, như bộ dữ liệu (YouNiverse: Large-Scale Channel and Video Metadata from English-Speaking YouTube) mặc dù đã thu thập ở năm 2021, nhưng ta hoàn toàn có thể lặp lại cách họ thu thập.

Đồng thời kết hợp một số phương pháp khác.

### Xây dựng một channel list.

Tạo tập hạt giống!

- Bước 1: Tạo một tập hạt giống (seed) gồm danh sách các kênh phổ biến ở Việt Nam, trong tất cả các lĩnh vực (cần định nghĩa lĩnh vực đó, đồng thời có thể dựa vào các lĩnh vực mà YouTube đã đặt ra - cần tìm kiếm sâu hơn.
- Bước 2: Sử dụng API hoặc một phương pháp nào đó (có thể là scrape tool) để mở rộng danh sách nhờ thuật toán đề xuất (thường là ở related channels).

Một kênh thứ ba.
- Dựa vào một trang web nào đó để có được danh sách kênh YouTube.

### Thu thập comment.
Ở bước này, ta có thể sử dụng một số cách.
- API (giới hạn)
- Scrape tool (không giới hạn, nhưng có thể bị chặn)
- Kết hợp cả hai (để có thể thu thập được nhiều hơn).
- Dùng Javascript để crawl.

... (Anything)

### Tổng hợp.
- Có thể tổng hợp từ một số tập dữ liệu đã có sẵn!

### Tiền xử lý!
- ...
