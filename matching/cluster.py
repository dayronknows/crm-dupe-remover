import pandas as pd
import networkx as nx
from rapidfuzz import fuzz
import time

def cluster_people(df_norm):
    start = time.time()
    cluster_map = {}
    cluster_id = 0

    # Cluster by exact email first (fast)
    email_groups = df_norm.groupby('email')
    for email, group in email_groups:
        if pd.isna(email) or email == '':
            continue
        indices = group.index.tolist()
        for idx in indices:
            cluster_map[idx] = cluster_id
        cluster_id += 1

    # Handle remaining records without email
    remaining_indices = [idx for idx in df_norm.index if idx not in cluster_map]

    # Fuzzy matching on remaining records by (first_name + last_name)
    # To keep it reasonable, use blocking on first letter of last_name
    name_blocks = {}
    for idx in remaining_indices:
        last_name = df_norm.at[idx, 'last_name']
        if pd.isna(last_name) or last_name == '':
            block_key = ''
        else:
            block_key = last_name[0].lower()
        name_blocks.setdefault(block_key, []).append(idx)

    # Use NetworkX to cluster by fuzzy matching
    G = nx.Graph()

    for block_key, indices in name_blocks.items():
        for i, idx1 in enumerate(indices):
            G.add_node(idx1)
            for idx2 in indices[i+1:]:
                # Compute combined fuzzy score on first + last name
                score_fn = fuzz.token_sort_ratio(str(df_norm.at[idx1, 'first_name']), str(df_norm.at[idx2, 'first_name']))
                score_ln = fuzz.token_sort_ratio(str(df_norm.at[idx1, 'last_name']), str(df_norm.at[idx2, 'last_name']))
                # Threshold can be tuned
                if (score_fn + score_ln) / 2 >= 85:
                    G.add_edge(idx1, idx2)

    # Assign cluster IDs to previously unmatched records
    for component in nx.connected_components(G):
        # If any node already in cluster_map, assign all to that cluster
        existing_clusters = {cluster_map[n] for n in component if n in cluster_map}
        if existing_clusters:
            assigned_cluster = existing_clusters.pop()
        else:
            assigned_cluster = cluster_id
            cluster_id += 1
        for node in component:
            cluster_map[node] = assigned_cluster

    # Assign cluster_id to DataFrame
    df_norm['cluster_id'] = df_norm.index.map(lambda x: cluster_map.get(x, -1))

    # Build clusters list
    clusters = []
    unique_cluster_ids = set(cluster_map.values())
    for cid in unique_cluster_ids:
        members = {idx for idx, clid in cluster_map.items() if clid == cid}
        clusters.append(members)

    print(f"[cluster_people] Total clustering time: {time.time() - start:.2f}s, clusters found: {len(clusters)}")
    return clusters, df_norm

def cluster_accounts(df):
    start = time.time()
    G = nx.Graph()

    # Blocking by first letter of account_name for speed
    blocks = {}
    for i, name in enumerate(df['account_name']):
        if pd.isna(name) or name == '':
            block_key = ''
        else:
            block_key = name[0].lower()
        blocks.setdefault(block_key, []).append(i)
        G.add_node(i)

    # Compare only within blocks (drastically reduce O(n^2))
    for block_key, indices in blocks.items():
        for i, idx1 in enumerate(indices):
            name1 = str(df.at[idx1, 'account_name'])
            for idx2 in indices[i+1:]:
                name2 = str(df.at[idx2, 'account_name'])
                ratio = fuzz.token_sort_ratio(name1, name2)
                if ratio > 85:
                    G.add_edge(idx1, idx2)

    clusters = list(nx.connected_components(G))

    # Annotate each record with cluster ID
    cluster_labels = [-1] * len(df)
    for cluster_id, cluster in enumerate(clusters):
        for idx in cluster:
            cluster_labels[idx] = cluster_id

    df_with_clusters = df.copy()
    df_with_clusters["cluster_id"] = cluster_labels

    print(f"[cluster_accounts] Total clustering time: {time.time() - start:.2f}s, clusters found: {len(clusters)}")
    return clusters, df_with_clusters
