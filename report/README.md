# Cách sử dụng folder này!

## Cài đặt môi trường liên quan

- Bắt buộc phải có docker. Bạn có thể tham khảo cài đặt ở [đây](https://docs.docker.com/engine/install/).
- Sau khi cài đặt docker bạn hãy chạy lệnh `docker build -t latex-small .` để tạo instance latex-small nhằm build file pdf.
- Bắt buộc phải cài `make` để build file pdf.

Có 3 con đường với windows:

```
choco install make
scoop install make
sudo apt install make (với WSL)
```
Hoặc với MacOS, nếu `make` không có sẵn hãy chạy lệnh.
```
xcode-select --install
```


## Cách build file pdf

- Chạy lệnh `make` để build file pdf.
- Chạy lệnh `make clean` để xóa file pdf.
- File pdf được lưu trong thư mục `output`.


## Cấu trúc folder này

```
.
├── Appendix              Thư mục chứa các mục như thư cảm ơn, thư cam đoan, phụ lục...
├── Chapter1              Thư mục chứa các nội chương 1
├── Chapter2              Thư mục chứa các nội chương 2
├── Chapter3              Thư mục chứa các nội chương 3
├── Chapter4              Thư mục chứa các nội chương 4
├── Chapter5              Thư mục chứa các nội chương 5
├── Dockerfile            File cài đặt docker (*)
├── Glossary              Thư mục chứa nội dung các từ viết tắt
├── Images                Thư mục chúa các hình ảnh
├── InPDFs                Thư mục chúa các file pdf (file pdf này là các trang pdf sẽ được gắn với file tex)
├── main.tex              File tex chính
├── Makefile              File make (*)
├── output                Thư mục chứa file pdf (thư mục kết quả)
├── References            Thư mục chứa các tài liệu tham khảo
├── SourceCode            Thư mục chúa các file mã code sẽ có trong báo cáo
└── Title                 Thư mục chứa nội dung trang bìa chính bìa phụ
```
