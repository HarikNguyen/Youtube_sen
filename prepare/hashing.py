import numpy as np
from datasketch import MinHash, LeanMinHash
import multiprocessing as mp
from tqdm import tqdm
import os
import pyarrow.parquet as pq

# ==========================================
# CẤU HÌNH
# ==========================================
INPUT_FILE = "norm_filtered_comments.parquet"
OUTPUT_DIR = "signatures"
TEXT_COLUMN = "comment"  # ⚠️ BẠN CẦN SỬA TÊN CỘT CHỨA VĂN BẢN TẠI ĐÂY
NUM_PERM = 128            
CHUNK_SIZE = 500000       # Đọc 500k dòng mỗi đợt
NUM_CORES = 3             

def get_minhash(text):
    """Hàm băm cho từng văn bản"""
    # Xử lý trường hợp dòng bị null/NaN
    if not text or not isinstance(text, str):
        text = ""
        
    tokens = text.strip().lower().split()
    m = MinHash(num_perm=NUM_PERM)
    for word in tokens:
        m.update(word.encode('utf8'))
        
    # Chuyển sang LeanMinHash để tối ưu bộ nhớ
    return LeanMinHash(m).hashvalues

def process_chunk(texts, chunk_id):
    """Xử lý đa luồng cho một khối dữ liệu"""
    with mp.Pool(NUM_CORES) as pool:
        signatures = list(pool.map(get_minhash, texts))
    
    # Lưu nhị phân xuống ổ cứng
    file_path = os.path.join(OUTPUT_DIR, f"chunk_{chunk_id}.npy")
    np.save(file_path, np.array(signatures, dtype=np.uint64))
    return file_path

def main():
    if not os.path.exists(OUTPUT_DIR):
        os.makedirs(OUTPUT_DIR)

    print(f"Bắt đầu phân tích file: {INPUT_FILE}...")
    
    # Mở file Parquet bằng PyArrow để hỗ trợ Streaming
    parquet_file = pq.ParquetFile(INPUT_FILE)
    total_rows = parquet_file.metadata.num_rows
    
    print(f"Tổng số dòng phát hiện: {total_rows:,}")
    chunk_count = 0
    
    # iter_batches: Cứu tinh của RAM, chỉ đọc 1 lượng dòng nhất định mỗi lần
    # columns=[TEXT_COLUMN]: Chỉ bóc tách cột văn bản, bỏ qua các cột metadata khác
    with tqdm(total=total_rows, desc="Tiến độ Hashing") as pbar:
        for batch in parquet_file.iter_batches(batch_size=CHUNK_SIZE, columns=[TEXT_COLUMN]):
            
            # Chuyển batch của cột đó thành danh sách chuỗi (list of strings)
            texts = batch.column(0).to_pylist()
            
            # Đẩy vào hàm xử lý đa luồng
            process_chunk(texts, chunk_count)
            
            chunk_count += 1
            pbar.update(len(texts))

    print(f"\nHoàn thành! Toàn bộ file .npy được lưu tại: {OUTPUT_DIR}")

if __name__ == "__main__":
    main()
