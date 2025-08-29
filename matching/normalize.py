# matching/normalize.py
import re
import pandas as pd
import phonenumbers

# --------- helpers ---------
_ALNUM = re.compile(r"[^a-z0-9]+")

def _norm_str(x) -> str:
    return str(x).strip() if isinstance(x, str) else ""

def _norm_lower(x) -> str:
    return _norm_str(x).lower()

def _alnum(s: str) -> str:
    return _ALNUM.sub("", _norm_lower(s))

def _domain_only(url: str) -> str:
    """
    Turn a URL into a bare domain:
      'https://www.Acme.co.uk/path?q=1' -> 'acme.co.uk'
      '', None -> ''
    """
    s = _norm_lower(url)
    if not s:
        return ""
    s = re.sub(r"^https?://", "", s)  # drop scheme
    s = s.split("/")[0]               # drop path
    s = re.sub(r"^www\.", "", s)      # drop www
    return s

def _normalize_email(email: str) -> str:
    """
    Lowercase, safe-trim. For gmail/googlemail, drop +tag in local part.
    """
    e = _norm_lower(email)
    if not e or "@" not in e:
        return e
    local, domain = e.split("@", 1)
    if domain in {"gmail.com", "googlemail.com"}:
        local = local.split("+", 1)[0]
    return f"{local}@{domain}"

def _normalize_phone(phone: str, region: str = "US") -> str:
    if not _norm_str(phone):
        return ""
    try:
        parsed = phonenumbers.parse(phone, region)
        if not phonenumbers.is_possible_number(parsed):
            return ""
        return phonenumbers.format_number(parsed, phonenumbers.PhoneNumberFormat.E164)
    except Exception:
        return ""

# --------- PEOPLE ---------
def normalize_people_df(leads_df: pd.DataFrame | None,
                        contacts_df: pd.DataFrame | None) -> pd.DataFrame:
    """
    Concatenate leads & contacts (if provided) and normalize key fields.
    Expects typical Zoho/CRM columns already lowercased/underscored by your loader.
    Adds helper columns: first_name_alnum, last_name_alnum
    """
    frames = []
    if leads_df is not None and len(leads_df):
        frames.append(leads_df.copy())
    if contacts_df is not None and len(contacts_df):
        frames.append(contacts_df.copy())
    if not frames:
        return pd.DataFrame()

    df = pd.concat(frames, ignore_index=True)

    # Ensure columns exist
    for col in ["first_name", "last_name", "email", "phone", "source_type"]:
        if col not in df.columns:
            df[col] = ""

    df["first_name"] = df["first_name"].apply(_norm_str)
    df["last_name"]  = df["last_name"].apply(_norm_str)
    df["email"]      = df["email"].apply(_normalize_email)
    df["phone"]      = df["phone"].apply(_normalize_phone)

    # Alnum helpers for robust blocking/fuzzy
    df["first_name_alnum"] = df["first_name"].apply(_alnum)
    df["last_name_alnum"]  = df["last_name"].apply(_alnum)

    return df.reset_index(drop=True)

# --------- ACCOUNTS ---------
def normalize_accounts_df(df: pd.DataFrame | None) -> pd.DataFrame:
    """
    Normalize account_name and website to a bare domain (website_domain).
    Keeps rows that have either a name or a domain.
    Adds helper: account_name_alnum
    """
    if df is None or len(df) == 0:
        return df

    out = df.copy()

    # Ensure account_name present (fallbacks if your CSV header varies)
    if "account_name" not in out.columns:
        # Try common alternatives
        lowered = {c.lower().replace(" ", ""): c for c in out.columns}
        src = lowered.get("accountname") or lowered.get("name") or lowered.get("company")
        out["account_name"] = out[src] if src else ""

    out["account_name"] = out["account_name"].apply(_norm_str)
    out["account_name_alnum"] = out["account_name"].apply(_alnum)

    # Website → bare domain
    web_col = None
    for c in out.columns:
        if c.lower().strip() in {"website", "url", "site"}:
            web_col = c
            break
    out["website_domain"] = out[web_col].apply(_domain_only) if web_col else ""

    # Keep if we have either name or domain
    keep_mask = (out["account_name"].str.len() > 0) | (out["website_domain"].str.len() > 0)
    out = out.loc[keep_mask].reset_index(drop=True)

    return out
