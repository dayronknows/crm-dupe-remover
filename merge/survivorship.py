from collections import Counter
import pandas as pd

def choose_master_people(clustered_df, clusters):
    merged = []

    for cluster in clusters:
        cluster_records = clustered_df.loc[list(cluster)]

        # Separate contacts and leads
        contacts = cluster_records[cluster_records['source_type'].str.lower() == 'contact']
        leads = cluster_records[cluster_records['source_type'].str.lower() == 'lead']

        if not contacts.empty:
            # Pick best contact as base by completeness
            completeness_scores = contacts.notnull().sum(axis=1)
            base_idx = completeness_scores.idxmax()
            base_record = contacts.loc[base_idx].copy()

            # Fill missing fields in base_record with info from leads
            for col in clustered_df.columns:
                if pd.isnull(base_record.get(col)) or base_record[col] == '':
                    lead_vals = leads[col].dropna()
                    if not lead_vals.empty:
                        base_record[col] = lead_vals.iloc[0]

            # Override names with mode from full cluster (contacts + leads)
            for field in ['first_name', 'last_name']:
                mode_vals = cluster_records[field].dropna()
                if not mode_vals.empty:
                    most_common = Counter(mode_vals).most_common(1)[0][0]
                    base_record[field] = most_common

            # Preserve source_type
            base_record['source_type'] = 'contact'

        else:
            # No contacts, just leads: pick best lead record by completeness
            completeness_scores = leads.notnull().sum(axis=1)
            base_idx = completeness_scores.idxmax()
            base_record = leads.loc[base_idx].copy()

            # Override names with mode from leads
            for field in ['first_name', 'last_name']:
                mode_vals = leads[field].dropna()
                if not mode_vals.empty:
                    most_common = Counter(mode_vals).most_common(1)[0][0]
                    base_record[field] = most_common

            # Preserve source_type
            base_record['source_type'] = 'lead'

        merged.append(base_record)

    merged_df = pd.DataFrame(merged).reset_index(drop=True)
    return merged_df


def choose_master_accounts(df, clusters):
    master_records = []
    for cluster in clusters:
        records = df.iloc[list(cluster)]
        master = records.iloc[0].copy()
        # Optionally, preserve or add any flags if needed here
        master_records.append(master)
    return pd.DataFrame(master_records).reset_index(drop=True)
