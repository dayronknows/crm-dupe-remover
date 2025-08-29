# app.py
"""
CRM De-Dupe & Merge Engine (MVP)
--------------------------------
Usage example:
 python app.py --leads examples/leads.csv --contacts examples/contacts.csv --accounts examples/accounts.csv --out out/
"""
import argparse
from matching.normalize import normalize_people_df, normalize_accounts_df
from matching.cluster import cluster_people, cluster_accounts
from merge.survivorship import choose_master_people, choose_master_accounts
from io_utils.readers import load_table
from io_utils.writers import (
   write_outputs_people,
   write_outputs_accounts,
   ensure_outdir,
)

def main():
   parser = argparse.ArgumentParser(description="CRM De-Dupe & Merge Engine (MVP)")
   parser.add_argument("--leads", type=str, required=False, help="Path to leads.csv")
   parser.add_argument("--contacts", type=str, required=False, help="Path to contacts.csv")
   parser.add_argument("--accounts", type=str, required=False, help="Path to accounts.csv")
   parser.add_argument("--out", type=str, required=True, help="Output directory for cleaned files")
   args = parser.parse_args()
   ensure_outdir(args.out)
# ---- Load ----
   leads_df = load_table(args.leads, kind="Lead") if args.leads else None
   contacts_df = load_table(args.contacts, kind="Contact") if args.contacts else None
   accounts_df = load_table(args.accounts, kind="Account") if args.accounts else None
# ---- Normalize ----
   people_norm = None
   if leads_df is not None or contacts_df is not None:
       people_norm = normalize_people_df(leads_df, contacts_df)
   accounts_norm = None
   if accounts_df is not None:
       accounts_norm = normalize_accounts_df(accounts_df)
# ---- Cluster & Merge (People: Leads + Contacts) ----
   if people_norm is not None and len(people_norm) > 0:
       clusters = cluster_people(people_norm)
       merged_people = choose_master_people(people_norm, clusters)
       write_outputs_people(merged_people, outdir=args.out)
       print(f"✅ People: {len(clusters)} clusters processed. Outputs written to {args.out}.")
# ---- Cluster & Merge (Accounts) ----
   if accounts_norm is not None and len(accounts_norm) > 0:
       a_clusters = cluster_accounts(accounts_norm)
       merged_accounts = choose_master_accounts(accounts_norm, a_clusters)
       write_outputs_accounts(merged_accounts, outdir=args.out)
       print(f"✅ Accounts: {len(a_clusters)} clusters processed. Outputs written to {args.out}.")
   print("🎉 Done.")

if __name__ == "__main__":
   main()