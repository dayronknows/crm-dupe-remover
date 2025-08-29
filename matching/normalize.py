import pandas as pd
import phonenumbers
import re
import tldextract

def normalize_name(name):
    return name.strip().lower() if isinstance(name, str) else ""

def normalize_email(email):
    if not isinstance(email, str):
        return ""
    return email.strip().lower().split("+")[0]

def normalize_phone(phone):
    try:
        parsed = phonenumbers.parse(phone, "US")
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except:
        return ""

def normalize_people_df(leads_df, contacts_df):
    df = pd.concat([leads_df, contacts_df], ignore_index=True)
    df["first_name"] = df["first_name"].apply(normalize_name)
    df["last_name"] = df["last_name"].apply(normalize_name)
    df["email"] = df["email"].apply(normalize_email)
    df["phone"] = df["phone"].apply(normalize_phone)
    return df

def normalize_accounts_df(accounts_df):
    df = accounts_df.copy()
    df["account_name"] = df["account_name"].apply(normalize_name)
    df["website"] = df["website"].apply(lambda x: tldextract.extract(x).domain if isinstance(x, str) else "")
    return df
