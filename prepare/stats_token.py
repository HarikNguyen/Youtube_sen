import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

# 1. Scan file
# lf = pl.scan_parquet('final_deduplicated_comments.parquet')
lf = pl.scan_parquet('deep_filtered_comments.parquet')

# 2. Tính token_count
lf = lf.with_columns(
    token_count = pl.col("comment").str.count_matches(r"\s+").fill_null(0) + 1
    # token_count = pl.col("comment").str.len_chars().fill_null(0)
)
stats = lf.select([
    pl.col("token_count").min().alias("Min"),
    pl.col("token_count").max().alias("Max"),
    pl.col("token_count").mean().alias("Mean"),
    pl.col("token_count").std().alias("Std"),
    pl.col("token_count").quantile(0.25).alias("Q1"),
    pl.col("token_count").median().alias("Q2"),
    pl.col("token_count").quantile(0.75).alias("Q3"),
]).collect()

# Trích xuất giá trị để in ấn
s = stats.to_dicts()[0] # Chuyển sang dict để truy cập nhanh
iqr = s["Q3"] - s["Q1"]

print(f"{' chỉ số thống kê ': ^40}")
print("-" * 40)
print(f"Giá trị nhỏ nhất (Min):  {s['Min']:,.2f}")
print(f"Giá trị lớn nhất (Max):  {s['Max']:,.2f}")
print(f"Trung bình (Mean):       {s['Mean']:,.2f}")
print(f"Độ lệch chuẩn (Std):     {s['Std']:,.2f}")
print("-" * 40)
print(f"Tứ phân vị thứ nhất (Q1): {s['Q1']:,.2f}")
print(f"Trung vị (Q2/Median):    {s['Q2']:,.2f}")
print(f"Tứ phân vị thứ ba (Q3):  {s['Q3']:,.2f}")
print(f"Khoảng biến thiên (IQR): {iqr:,.2f}")
print("-" * 40)
# 3. Lấy mẫu an toàn: 
# Chỉ lấy cột token_count (rất nhẹ), collect về RAM rồi mới sample
df_sample = (
    lf.select("token_count")
    .collect(engine="streaming")
    .sample(n=1_000_000)
)

# 4. Vẽ biểu đồ
plt.figure(figsize=(12, 6))
sns.violinplot(data=df_sample.to_pandas(), x="token_count", color="skyblue", inner="box", bw_adjust=1.5, cut=0)

plt.title('Phân phối Token (Random Sample 1000k rows)', fontsize=14)
plt.grid(axis='x', linestyle='--', alpha=0.6)
plt.show()
