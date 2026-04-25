import fasttext

txt_train_file = "fasttext_train.txt"
model = fasttext.train_supervised(
    input=txt_train_file,
    lr=0.5,  # Learning rate: 0.5 giúp model học các pattern nhanh hơn
    epoch=50,  # Số vòng lặp (với dataset nhỏ, 25-50 là hợp lý)
    wordNgrams=2,  # Học các cặp 2 từ liền nhau (vd: "đại dịch", "stupid people")
    minn=2,  # Cắt ký tự n-gram ngắn nhất = 2 (Giúp bắt "hk", "ko", "k ")
    maxn=7,  # Cắt ký tự n-gram dài nhất = 5 (Giúp bắt "chời", "khum")
    dim=50,  # Số chiều vector (50 là đủ nhẹ và nhanh)
    loss="hs",  # Dùng Hierarchical Softmax tối ưu tốc độ
)

print("-> Huấn luyện thành công!\n")

result = model.test(txt_train_file)
print("3. Báo cáo đánh giá (Trên tập Train):")
print(f" - Số lượng mẫu: {result[0]}")
print(f" - Độ chính xác (Precision): {result[1]:.4f}")
print(f" - Độ phủ (Recall):      {result[2]:.4f}\n")

model_name = "my_slang_classifier.bin"
model.save_model(model_name)
print(f"-> Đã lưu mô hình tại: {model_name}\n")
