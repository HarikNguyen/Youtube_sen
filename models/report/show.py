import pandas as pd
import matplotlib.pyplot as plt

MODEL = "mmodernbert"
# Load the data from the CSV file
df = pd.read_csv(f"{MODEL}/training_metrics.csv")

# Create figure and axis
fig, ax1 = plt.subplots(figsize=(10, 6))

# Plot Loss on the left axis
color_train_loss = '#1f77b4'
color_val_loss = '#aec7e8'
line1 = ax1.plot(df['epoch'], df['train_loss'], color=color_train_loss, marker='o', label='Train Loss')
line2 = ax1.plot(df['epoch'], df['val_loss'], color=color_val_loss, marker='s', linestyle='--', label='Val Loss')
ax1.set_xlabel('Epoch')
ax1.set_ylabel('Loss', color='black')
ax1.tick_params(axis='y', labelcolor='black')
ax1.set_xticks(df['epoch'])
ax1.grid(True, linestyle=':', alpha=0.6)

# Create a second axis sharing the same x-axis for Accuracy/F1
ax2 = ax1.twinx()
color_f1 = '#2ca02c'
color_acc = '#ff7f0e'
line3 = ax2.plot(df['epoch'], df['f1-mac'], color=color_f1, marker='^', label='F1 (Macro)')
line4 = ax2.plot(df['epoch'], df['acc_balanced'], color=color_acc, marker='d', linestyle='-.', label='Balanced Acc')
ax2.set_ylabel('Accuracy / F1 Score', color='black')
ax2.tick_params(axis='y', labelcolor='black')

# Combine legends from both axes
lines = line1 + line2 + line3 + line4
labels = [l.get_label() for l in lines]
ax1.legend(lines, labels, loc='center left')

plt.title('Training and Validation Metrics across Epochs')
fig.tight_layout()

# Save the plot
plt.savefig(f'{MODEL}/epoch_metrics_chart.png', dpi=300)
print(f"Chart saved as {MODEL}/epoch_metrics_chart.png")
