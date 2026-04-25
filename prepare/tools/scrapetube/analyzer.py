import pandas as pd
import matplotlib.pyplot as plt

df = pd.read_csv("res_updated.csv")

# Biểu đồ 1: Số lượng video theo Category
plt.figure(figsize=(10, 6))
df["category"].value_counts().plot(kind="bar", color="skyblue")
plt.title("Số lượng video theo Category")
plt.xlabel("Category")
plt.ylabel("Số lượng video")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("distribution_category.png")

# Biểu đồ 2: Top 10 kênh có nhiều video nhất (theo title/name)
plt.figure(figsize=(10, 6))
df["title"].value_counts().head(10).plot(kind="barh", color="salmon")
plt.title("Top 10 kênh có số lượng video nhiều nhất")
plt.xlabel("Số lượng video")
plt.ylabel("Tên kênh")
plt.gca().invert_yaxis()
plt.tight_layout()
plt.savefig("distribution_channel.png")
