import streamlit as st
import pandas as pd
import time
import warnings
import logging

# Suppress pandas dtype warning
warnings.filterwarnings("ignore", category=pd.errors.DtypeWarning)

# Suppress tornado WebSocketClosedError logs
logging.getLogger("tornado.application").setLevel(logging.ERROR)

from matching.normalize import normalize_people_df, normalize_accounts_df
from matching.cluster import cluster_people, cluster_accounts
from merge.survivorship import choose_master_people, choose_master_accounts

st.set_page_config(layout="wide", page_title="CRM De-Dupe Dashboard")
st.title("🧹 CRM De-Dupe & Merge Dashboard")

st.sidebar.header("📁 Upload CRM CSVs")
leads_file = st.sidebar.file_uploader("Upload Leads CSV", type=["csv"])
contacts_file = st.sidebar.file_uploader("Upload Contacts CSV", type=["csv"])
accounts_file = st.sidebar.file_uploader("Upload Accounts CSV", type=["csv"])

skip_people = st.sidebar.checkbox("🚫 Skip People De-Duplication", value=False)
skip_accounts = st.sidebar.checkbox("🚫 Skip Account De-Duplication", value=False)

def clean_columns(df):
    df.columns = df.columns.str.strip().str.lower().str.replace(" ", "_")
    return df

def load_uploaded_csv(uploaded_file, kind=""):
    if uploaded_file:
        df = pd.read_csv(uploaded_file, low_memory=False)
        df = clean_columns(df)
        df["source_type"] = kind
        return df
    return None

# Main button to run deduplication
if st.sidebar.button("🚀 Run Deduplication"):
    total_start = time.time()

    with st.spinner("Loading and normalizing data..."):
        load_start = time.time()

        leads_df = load_uploaded_csv(leads_file, "lead")
        contacts_df = load_uploaded_csv(contacts_file, "contact")
        accounts_df = load_uploaded_csv(accounts_file, "account")

        leads_cols = leads_df.columns.tolist() if leads_df is not None else []
        contacts_cols = contacts_df.columns.tolist() if contacts_df is not None else []
        accounts_cols = accounts_df.columns.tolist() if accounts_df is not None else []

        people_norm = None
        if leads_df is not None or contacts_df is not None:
            people_norm = normalize_people_df(leads_df, contacts_df)

        accounts_norm = normalize_accounts_df(accounts_df) if accounts_df is not None else None

        load_end = time.time()
        st.success(f"✅ Data loading took {load_end - load_start:.2f} seconds")

    # Save to session
    st.session_state.update({
        "people_norm": people_norm,
        "accounts_norm": accounts_norm,
        "leads_cols": leads_cols,
        "contacts_cols": contacts_cols,
        "accounts_cols": accounts_cols,
    })

    # ---- People Deduplication ----
    if not skip_people:
        if people_norm is not None and len(people_norm) > 0:
            with st.spinner("Clustering and merging people..."):
                people_start = time.time()
                people_clusters, clustered_df = cluster_people(people_norm)
                merged_people = choose_master_people(clustered_df, people_clusters)
                merged_people_df = pd.DataFrame(merged_people)
                people_end = time.time()
                st.success(f"👥 People deduplication took {people_end - people_start:.2f} seconds")

            st.session_state.update({
                "people_clusters": people_clusters,
                "people_clustered_df": clustered_df,
                "merged_people_df": merged_people_df
            })
    else:
        st.warning("⚠️ People deduplication skipped.")

    # ---- Accounts Deduplication (optional) ----
    if accounts_norm is not None and len(accounts_norm) > 0:
        st.info(f"📊 Accounts loaded: {len(accounts_norm)} rows")
        st.info(f"🔢 Unique Account Names: {accounts_norm['account_name'].nunique()}")

        # Show top repeated account names (optional)
        if st.checkbox("🔍 Show Top Repeated Account Names"):
            top_accounts = accounts_norm['account_name'].value_counts().head(10)
            st.dataframe(top_accounts.reset_index().rename(columns={'index': 'account_name', 'account_name': 'count'}))

        if not skip_accounts:
            with st.spinner("Clustering and merging accounts..."):
                acc_start = time.time()
                account_clusters, accounts_clustered_df = cluster_accounts(accounts_norm)
                merged_accounts = choose_master_accounts(accounts_clustered_df, account_clusters)
                merged_accounts_df = pd.DataFrame(merged_accounts)
                acc_end = time.time()
                st.success(f"🏢 Accounts deduplication took {acc_end - acc_start:.2f} seconds")

            st.session_state.update({
                "accounts_clustered_df": accounts_clustered_df,
                "merged_accounts_df": merged_accounts_df
            })
        else:
            st.warning("⚠️ Account deduplication skipped.")

    total_end = time.time()
    st.info(f"⏱️ Total deduplication time: {total_end - total_start:.2f} seconds")

# ---- DISPLAY RESULTS ----

people_clustered_df = st.session_state.get("people_clustered_df")
merged_people_df = st.session_state.get("merged_people_df")
leads_cols = st.session_state.get("leads_cols")
contacts_cols = st.session_state.get("contacts_cols")

if merged_people_df is not None:
    # Filter contacts & leads separately with intersection on original columns
    contacts_final = merged_people_df.loc[merged_people_df["source_type"] == "contact", merged_people_df.columns.intersection(contacts_cols)] if contacts_cols else pd.DataFrame()
    leads_final = merged_people_df.loc[merged_people_df["source_type"] == "lead", merged_people_df.columns.intersection(leads_cols)] if leads_cols else pd.DataFrame()

    # Remove leads that are already in contacts by record_id
    if not contacts_final.empty and not leads_final.empty and 'record_id' in leads_final.columns and 'record_id' in contacts_final.columns:
        leads_final = leads_final[~leads_final["record_id"].isin(contacts_final["record_id"])]

    st.subheader(f"📋 Deduplicated Leads ({len(leads_final)}) and Contacts ({len(contacts_final)})")
    col1, col2 = st.columns(2)
    with col1:
        st.dataframe(leads_final)
        st.download_button("⬇️ Download Leads CSV", leads_final.to_csv(index=False), "leads_deduped.csv")
    with col2:
        st.dataframe(contacts_final)
        st.download_button("⬇️ Download Contacts CSV", contacts_final.to_csv(index=False), "contacts_deduped.csv")

# People clusters expandable
if people_clustered_df is not None:
    dupe_ids = people_clustered_df["cluster_id"].value_counts()
    dupe_ids = dupe_ids[dupe_ids > 1].index
    st.subheader(f"🔗 Duplicate People Clusters ({len(dupe_ids)} groups)")

    # Wrap all clusters inside one big expander
    with st.expander("🔽 Expand All People Clusters"):
        for cluster_id in sorted(dupe_ids):
            cluster_records = people_clustered_df[people_clustered_df["cluster_id"] == cluster_id]
            label = cluster_records.iloc[0][["first_name", "last_name"]].str.cat(sep=" ")
            with st.expander(f"Cluster {cluster_id} - {label} ({len(cluster_records)} records)"):
                st.dataframe(cluster_records)

    # Add people clusters CSV download button here:
    csv_people_clusters = people_clustered_df.to_csv(index=False)
    st.download_button(
        label="⬇️ Download People Clusters CSV",
        data=csv_people_clusters,
        file_name="people_clusters.csv",
        mime="text/csv"
    )

# Accounts display
accounts_clustered_df = st.session_state.get("accounts_clustered_df")
merged_accounts_df = st.session_state.get("merged_accounts_df")
accounts_cols = st.session_state.get("accounts_cols")

if merged_accounts_df is not None:
    accounts_final = merged_accounts_df.loc[:, merged_accounts_df.columns.intersection(accounts_cols)] if accounts_cols else merged_accounts_df
    st.subheader(f"🏢 Deduplicated Accounts ({len(accounts_final)})")
    st.dataframe(accounts_final)
    st.download_button("⬇️ Download Accounts CSV", accounts_final.to_csv(index=False), "accounts_deduped.csv")

if accounts_clustered_df is not None:
    acc_dupes = accounts_clustered_df["cluster_id"].value_counts()
    acc_dupes = acc_dupes[acc_dupes > 1].index
    st.subheader(f"🏷️ Duplicate Account Clusters ({len(acc_dupes)} groups)")

    # Wrap all account clusters inside one big expander
    with st.expander("🔽 Expand All Account Clusters"):
        for cluster_id in sorted(acc_dupes):
            cluster_data = accounts_clustered_df[accounts_clustered_df["cluster_id"] == cluster_id]
            name = cluster_data.iloc[0].get("account_name", f"Cluster {cluster_id}")
            with st.expander(f"Cluster {cluster_id} - {name} ({len(cluster_data)} records)"):
                st.dataframe(cluster_data)

    # Add accounts clusters CSV download button here:
    csv_accounts_clusters = accounts_clustered_df.to_csv(index=False)
    st.download_button(
        label="⬇️ Download Account Clusters CSV",
        data=csv_accounts_clusters,
        file_name="account_clusters.csv",
        mime="text/csv"
    )
