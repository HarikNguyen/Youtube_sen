import json
import pandas as pd

with open('seed.json', 'r', encoding='utf-8') as f:
    seed_data = json.load(f)

seed_list = []
for category, channels in seed_data.items():
    for channel in channels:
        # channel[0] là channel_id, channel[1] là name
        seed_list.append({
            'category': category,
            'name': channel[1],
            'channel_id': channel[0]
        })

df_seed = pd.DataFrame(seed_list)

df_unique = pd.read_csv('unique.csv')

df_combined = pd.concat([df_seed, df_unique], ignore_index=True)

df_final = df_combined.drop_duplicates(subset=['channel_id'], keep='first')

df_final.to_csv('merged_channels.csv', index=False, encoding='utf-8-sig')

print(f"Completed! ({len(df_final)} channels). Saved to merged_channels.csv")
