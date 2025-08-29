# matching/cluster.py
import time
import re
import pandas as pd
import networkx as nx
from rapidfuzz import fuzz

# --------- helpers ---------
_ALNUM = re.compile(r"[^a-z0-9]+")

def _alnum(s: str) -> str:
    return _ALNUM.sub("", str(s).strip().lower())

# --------- PEOPLE ---------
def cluster_people(df_norm: pd.DataFrame):
    """
    1) Exact pre-cluster by email (when present)
    2) Fuzzy by (first_name, last_name) within tight blocks (LN initial + FN initial)
       - require >=3 alnum chars in both names for fuzzy
       - threshold tuned to 88 to reduce chain merges
    Returns: (clusters: list[set[int]], df_with_cluster_id)
    """
    start = time.time()
    df = df_norm.copy()

    # Ensure helpers exist
    for col in ["first_name", "last_name", "email"]:
        if col not in df.columns:
            df[col] = ""
    if "first_name_alnum" not in df.columns:
        df["first_name_alnum"] = df["first_name"].apply(_alnum)
    if "last_name_alnum" not in df.columns:
        df["last_name_alnum"] = df["last_name"].apply(_alnum)

    cluster_map: dict[int, int] = {}
    next_cid = 0

    # 1) Exact by email
    if "email" in df.columns:
        for em, grp in df.groupby(df["email"].where(df["email"] != "", None), dropna=True):
            for idx in grp.index:
                cluster_map[idx] = next_cid
            next_cid += 1

    # 2) Fuzzy by name for remaining
    remaining = [i for i in df.index if i not in cluster_map]

    # Block by LN initial + FN initial (tight)
    blocks: dict[str, list[int]] = {}
    for i in remaining:
        ln = df.at[i, "last_name_alnum"][:1] if isinstance(df.at[i, "last_name_alnum"], str) else ""
        fn = df.at[i, "first_name_alnum"][:1] if isinstance(df.at[i, "first_name_alnum"], str) else ""
        key = f"{ln}{fn}"  # e.g., 'sd' for Smith, Daniel
        blocks.setdefault(key, []).append(i)

    G = nx.Graph()
    for idxs in blocks.values():
        for i in idxs:
            G.add_node(i)
        for a, i in enumerate(idxs):
            f1 = df.at[i, "first_name"]
            l1 = df.at[i, "last_name"]
            if len(_alnum(f1)) < 3 or len(_alnum(l1)) < 3:
                continue  # ignore too-short tokens
            for j in idxs[a+1:]:
                f2 = df.at[j, "first_name"]
                l2 = df.at[j, "last_name"]
                if len(_alnum(f2)) < 3 or len(_alnum(l2)) < 3:
                    continue
                # average of first & last name similarity
                s_fn = fuzz.token_sort_ratio(f1.lower(), f2.lower())
                s_ln = fuzz.token_sort_ratio(l1.lower(), l2.lower())
                if (s_fn + s_ln) / 2 >= 88:
                    G.add_edge(i, j)

    for comp in nx.connected_components(G):
        cid = next_cid
        for node in comp:
            cluster_map[node] = cid
        next_cid += 1

    # Singletons
    for i in remaining:
        if i not in cluster_map:
            cluster_map[i] = next_cid
            next_cid += 1

    df["cluster_id"] = df.index.map(cluster_map.get)

    # Build cluster sets
    clusters = []
    for cid in sorted(set(cluster_map.values())):
        clusters.append({i for i, c in cluster_map.items() if c == cid})

    print(f"[cluster_people] time: {time.time()-start:.2f}s, clusters: {len(clusters)}")
    return clusters, df

# --------- ACCOUNTS ---------
def cluster_accounts(df: pd.DataFrame):
    """
    1) Exact pre-cluster by website_domain (when present)
    2) Fuzzy by account_name within 2-char alnum blocks
       - ignore names with <3 alnum chars for fuzzy
       - threshold 90 to prevent chain merges
    Returns: (clusters: list[set[int]], df_with_cluster_id)
    """
    start = time.time()
    dfa = df.copy()

    if "account_name" not in dfa.columns:
        raise KeyError("Expected 'account_name' in accounts dataframe.")
    if "website_domain" not in dfa.columns:
        dfa["website_domain"] = ""
    if "account_name_alnum" not in dfa.columns:
        dfa["account_name_alnum"] = dfa["account_name"].apply(_alnum)

    cluster_map: dict[int, int] = {}
    next_cid = 0

    # 1) Exact by website domain
    if dfa["website_domain"].str.len().gt(0).any():
        for dom, grp in dfa.groupby(dfa["website_domain"].where(dfa["website_domain"] != "", None), dropna=True):
            for idx in grp.index:
                cluster_map[idx] = next_cid
            next_cid += 1

    # 2) Fuzzy by name within small blocks
    remaining = [i for i in dfa.index if i not in cluster_map]

    blocks: dict[str, list[int]] = {}
    for i in remaining:
        key = dfa.at[i, "account_name_alnum"][:2] if isinstance(dfa.at[i, "account_name_alnum"], str) else ""
        blocks.setdefault(key, []).append(i)

    G = nx.Graph()
    for idxs in blocks.values():
        for i in idxs:
            G.add_node(i)
        for a, i in enumerate(idxs):
            n1 = str(dfa.at[i, "account_name"]).strip()
            a1 = dfa.at[i, "account_name_alnum"]
            if len(a1) < 3:
                continue
            for j in idxs[a+1:]:
                n2 = str(dfa.at[j, "account_name"]).strip()
                a2 = dfa.at[j, "account_name_alnum"]
                if len(a2) < 3:
                    continue
                score = fuzz.token_sort_ratio(n1.lower(), n2.lower())
                if score >= 90:
                    G.add_edge(i, j)

    for comp in nx.connected_components(G):
        cid = next_cid
        for node in comp:
            cluster_map[node] = cid
        next_cid += 1

    for i in remaining:
        if i not in cluster_map:
            cluster_map[i] = next_cid
            next_cid += 1

    dfa["cluster_id"] = dfa.index.map(cluster_map.get)

    clusters = []
    for cid in sorted(set(cluster_map.values())):
        clusters.append({i for i, c in cluster_map.items() if c == cid})

    print(f"[cluster_accounts] time: {time.time()-start:.2f}s, clusters: {len(clusters)}")
    return clusters, dfa
