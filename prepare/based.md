Các bình luận của người dùng trên YouTube dù cho ở khía cạnh nào đó thì thông thường rất hỗn loạn, chẳng hạn như chứa nhiều URLs, hashtags, emojis, icons, từ viết tắt, tiếng lóng, và cực kỳ sai ngữ pháp nghiêm trọng.

Và trong tình huống như vậy, tiền xử lý dữ liệu là điều bắt buộc để tăng cường chất lượng dữ liệu đầu vào để phân tích hoặc huấn luyện mô hình.

Với các phương pháp Machine Learning truyền thống thì công việc thường là tập trung vào các công việc 'chuẩn hóa' như lowercase, loại bỏ space và các ký tự xuống dòng thừa, loại bỏ các điểm dữ liệu trùng lặp, loại bỏ stop words, tháo gỡ các từ viết tắt, đưa icon/emoji về dạng text, chọn n-grams, tách từ, ... cuối cùng là tokenize. Tất cả nhằm mục đích tạo ra các token có không gian chuẩn nhất để đưa vào mô hình.

Nhưng đối với các mô hình hiện đại dựa trên transformer based, thì các bước không cần thiết như loại bỏ stop words, hay tách từ, vì các mô hình đã được thiết kế để chúng có thể tự động học nó.

(Sentiment Analysis Using a Large Language Model–Based Approach to Detect Opioids Mixed With Other Substances Via Social Media: Method Development and Validation)
((Muhammad Ahmad, MPhil; Ildar Batyrshin, PhD; Grigori Sidorov, PhD))

----

Các tiền đề:

- 


----


MinHash de-duplication across the entire dataset to remove near duplicate documents.

Text extraction and cleaning. We process the raw HTML content for non-truncated web documents to extract
high-quality diverse text. To do so, we build a custom parser that extracts the HTML content and optimizes
for precision in boilerplate removal and content recall. We evaluate our parser’s quality in human evaluations,
comparing it with popular third-party HTML parsers that optimize for article-like content, and found it
to perform favorably. We carefully process HTML pages with mathematics and code content to preserve
the structure of that content. We maintain the image alt attribute text since mathematical content is often
represented as pre-rendered images where the math is also provided in the alt attribute. We experimentally
evaluate different cleaning configurations. We find markdown is harmful to the performance of a model that
is primarily trained on web data compared to plain text, so we remove all markdown markers.
De-duplication. We apply several rounds of de-duplication at the URL, document, and line level:
• URL-level de-duplication. We perform URL-level de-duplication across the entire dataset. We keep the
most recent version for pages corresponding to each URL.
• Document-level de-duplication. We perform global MinHash (Broder, 1997) de-duplication across the
entire dataset to remove near duplicate documents.
• Line-level de-duplication. We perform aggressive line-level de-duplication similar to ccNet (Wenzek
et al., 2019). We remove lines that appeared more than 6 times in each bucket of 30M documents.
Although our manual qualitative analysis showed that the line-level de-duplication removes not only
leftover boilerplate from various websites such as navigation menus, cookie warnings, but also frequent
high-quality text, our empirical evaluations showed strong improvements.
Heuristic filtering. We develop heuristics to remove additional low-quality documents, outliers, and documents
with excessive repetitions. Some examples of heuristics include:
• We use duplicated n-gram coverage ratio (Rae et al., 2021) to remove lines that consist of repeated
content such as logging or error messages. Those lines could be very long and unique, hence cannot be
filtered by line-dedup.
• We use “dirty word” counting (Raffel et al., 2020) to filter out adult websites that are not covered by
domain block lists.
• We use a token-distribution Kullback-Leibler divergence to filter out documents containing excessive
numbers of outlier tokens compared to the training corpus distribution.
