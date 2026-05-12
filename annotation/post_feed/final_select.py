import pandas as pd

TARGET_TOTAL = 2000000
QUALITY_FLOOR = 0.60
BASE_STEP = 0.02

threshold_map = {
    # MINORITY CLASSES: Optimized for Data Retention
    # Lowering thresholds to ensure sufficient representation of rare labels
    'grief': 0.45, 'nervousness': 0.45, 'embarrassment': 0.50,
    'remorse': 0.55, 'pride': 0.60, 'relief': 0.60,

    # BALANCED CLASSES: Maintaining Precision-Recall Equilibrium
    # Mid-range thresholds for labels with moderate frequency
    'disgust': 0.65, 'confusion': 0.65, 'fear': 0.65, 'disappointment': 0.70,
    'caring': 0.70, 'desire': 0.70, 'optimism': 0.75, 'realization': 0.75,
    'surprise': 0.75, 'excitement': 0.75,

    # MAJORITY CLASSES: Optimized for High Confidence
    # Higher standards to filter the most reliable samples from large pools
    'annoyance': 0.80, 'disapproval': 0.80, 'curiosity': 0.80, 
    'anger': 0.80, 'sadness': 0.80, 'amusement': 0.80,
    'admiration': 0.82, 'joy': 0.82, 'approval': 0.82,

    # DENSITY CONTROL: High-Strictness Filtering
    # Strict thresholds to prevent dataset imbalance and ensure elite quality
    'love': 0.85, 'gratitude': 0.85, 'neutral': 0.88
}

def main():
    df = pd.read_parquet('pred_5p5M_3.parquet')
    current_thresholds = threshold_map.copy()

    # 1st filter
    threshold_series = df['labels'].map(current_thresholds)
    df_official = df[df['confidence'] >= threshold_series].copy()
    current_count = len(df_official)
    print(f"Num of labels (1st): {len(df_official['labels'].unique())}")
    print(f"Num of samples (1st): {len(df_official)}")
    # Add more data to reach TARGET_TOTAL
    while current_count < TARGET_TOTAL:
        print(f"Lower threshold to get closer to {TARGET_TOTAL}...")
        counts = df_official['labels'].value_counts()
        avg_count = counts.mean()

        for label in current_thresholds:
            a_count = counts.get(label, 1)
            
            x_rate = avg_count / a_count
            x_rate = min(x_rate, 5) # max step = 0.1

            actual_step = BASE_STEP * x_rate
            
            if current_thresholds[label] > QUALITY_FLOOR:
                current_thresholds[label] = round(max(current_thresholds[label] - actual_step, QUALITY_FLOOR), 3)

        threshold_series = df['labels'].map(current_thresholds)
        df_official = df[df['confidence'] >= threshold_series].copy()

        new_count = len(df_official)
        print(f"Num of labels (now): {len(df_official['labels'].unique())}")
        print(f"Num of samples (now): {len(df_official)}")

        if new_count <= current_count:
            break
        current_count = new_count


    # stats
    n_o_l = df_official['labels'].value_counts()
    cm_o_l = df_official.groupby('labels').confidence.mean()
    c_m_o_l = df_official.groupby('labels').confidence.median()
    cM_o_l = df_official.groupby('labels').confidence.max()
    
    print(pd.DataFrame({
        'label': n_o_l.index,
        'count': n_o_l.values,
        'confidence_mean': cm_o_l.values,
        'confidence_median': c_m_o_l.values,
        'confidence_max': cM_o_l.values
    }))
    
    # save
    df_official = df_official.drop('confidence', axis=1)
    df_official.to_parquet("final_2M.parquet")


if __name__ == "__main__":
    main()
