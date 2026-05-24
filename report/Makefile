# --- CẤU HÌNH BIẾN ---
MAIN = main
OUT_DIR = output
DOCKER_IMG = latex-small
REF_BE = biber

# Khai báo các target không phải là file
.PHONY: all clean directories

# --- LỆNH THỰC THI CHÍNH ---
all: directories
	# Chạy container Docker để biên dịch
	docker run --rm -v $(CURDIR):/workdir -w /workdir $(DOCKER_IMG) sh -c \
    "pdflatex -output-directory=$(OUT_DIR) $(MAIN).tex && \
    ${REF_BE} --output-directory $(OUT_DIR) $(MAIN) && \
	makeglossaries -d $(OUT_DIR) $(MAIN) && \
    pdflatex -output-directory=$(OUT_DIR) $(MAIN).tex && \
    pdflatex -output-directory=$(OUT_DIR) $(MAIN).tex"

# --- TỰ ĐỘNG QUÉT VÀ TẠO TOÀN BỘ THƯ MỤC CON ---
directories:
	# 1. Tạo thư mục output gốc nếu chưa có
	mkdir -p $(OUT_DIR)
	# 2. Tự động quét các thư mục con hiện tại (loại trừ output, images, .git...) 
	# và tạo cấu trúc tương ứng bên trong thư mục output
	@find . -maxdepth 1 -type d ! -name "." ! -name "$(OUT_DIR)" ! -name "images" ! -name "output" ! -name ".*" | while read -r dir; do \
		mkdir -p "$(OUT_DIR)/$$dir"; \
	done

# --- DỌN DẸP FILE RÁC ---
clean:
	rm -rf $(OUT_DIR)/*
