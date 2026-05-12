import pandas as pd
from sklearn.cluster import KMeans
from sklearn.metrics import pairwise_distances_argmin_min
from sklearn.feature_extraction.text import TfidfVectorizer

TARGET_COUNT = 500  # the number of samples you want to balance 28 classes
df = pd.read_csv('ft_seed.csv')

def balance_pipeline(df, target_n):
    tfidf = TfidfVectorizer(max_features=5000)
    X = tfidf.fit_transform(df['text'].astype(str))
    
    final_list = []
    need_more = {}

    for label in df['labels'].unique():
        group = df[df['labels'] == label]
        indices = group.index
        X_group = X[indices]
        
        if len(group) >= target_n:
            # Too Excessive => Use Cluster Centroids to get target_n samples representative
            kmeans = KMeans(n_clusters=target_n, random_state=42, n_init=5)
            kmeans.fit(X_group)
            closest, _ = pairwise_distances_argmin_min(kmeans.cluster_centers_, X_group)
            final_list.append(df.loc[indices[closest]])
            print(f"Label [{label}]: Compressed to {target_n}")
        else:
            # Lack of samples => Take the whole of the group
            final_list.append(group)
            need_more[label] = target_n - len(group)
            print(f"Label [{label}]: With {len(group)} item at the moment, need {need_more[label]} more!")
            
    return pd.concat(final_list), need_more

# Run and save
df_balanced, missing_report = balance_pipeline(df, TARGET_COUNT)
df_balanced.to_csv('balanced_seed.csv', index=False)
