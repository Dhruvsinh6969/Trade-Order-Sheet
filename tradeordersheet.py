import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from googleapiclient.discovery import build
from email.mime.text import MIMEText
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
import base64
import os, pickle
from calendar import monthrange
import json

# ========== CONFIGURATION ==========
GOOGLE_SHEET_ID = st.secrets["google_sheet_id"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/gmail.send"
]

# --- Google Sheets Auth (Service Account) ---
creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=SCOPES
)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEET_ID)


# --- Gmail Auth (OAuth via secrets, no credentials.json) ---
def get_gmail_service():
    creds = None

    # 🔑 Try to load token from st.secrets (base64)
    if "token" in st.secrets and "pickle_b64" in st.secrets["token"]:
        import base64, pickle
        token_bytes = base64.b64decode(st.secrets["token"]["pickle_b64"])
        creds = pickle.loads(token_bytes)

    # Refresh or re-run flow if invalid
    if not creds or not creds.valid:
        if creds and creds.expired and creds.refresh_token:
            creds.refresh(Request())
        else:
            client_config = {
                "installed": {
                    "client_id": st.secrets["gmail_oauth"]["client_id"],
                    "client_secret": st.secrets["gmail_oauth"]["client_secret"],
                    "auth_uri": st.secrets["gmail_oauth"]["auth_uri"],
                    "token_uri": st.secrets["gmail_oauth"]["token_uri"],
                }
            }
            flow = InstalledAppFlow.from_client_config(client_config, SCOPES)
            creds = flow.run_local_server(port=0)

        # Save new token locally (for dev use only, not on Streamlit Cloud)
        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build("gmail", "v1", credentials=creds)


# ✅ cache gmail service so it’s initialized only once
@st.cache_resource
def get_gmail_service_cached():
    return get_gmail_service()


gmail_service = get_gmail_service_cached()


# ========== HELPERS ==========
def to_num(x, default=0):
    try:
        if pd.isna(x):
            return default
        if isinstance(x, (int, float)):
            return int(x)
        s = str(x).strip().replace(",", "")
        if s == "" or s.lower() == "nan":
            return default
        return int(float(s))
    except:
        return default


def append_row(tab, data_dict):
    ws = sheet.worksheet(tab)
    headers = ws.row_values(1)
    full_headers = [
        "Timestamp", "Order Date", "Employee Name", "Party", "Store Name", "City",
        "Category", "SKU", "Qty", "SOH", "Remarks",
        "Last Month Net Sales", "Running Month Net Sales", "Flag"
    ]
    if not headers:
        ws.insert_row(full_headers, 1)
        headers = full_headers
    row = [data_dict.get(col, "") for col in headers]
    ws.append_row(row)


def send_email(to, subject, body):
    if not to or str(to).strip() == "":
        return # skip if no recipients

    message = MIMEText(body)
    message['to'] = to
    message['subject'] = subject
    create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
    gmail_service.users().messages().send(userId="me", body=create_message).execute()


@st.cache_data(ttl=30)
def load_data(tab):
    ws = sheet.worksheet(tab)
    df = pd.DataFrame(ws.get_all_records())
    if not df.empty:
        obj_cols = df.select_dtypes(include="object").columns
        for c in obj_cols:
            df[c] = df[c].astype(str).str.strip()
    return df


# ========== LOAD MASTERS ==========
st.title("📦 Trade Order Form")

if st.button("🔄 Re-load Google Sheets"):
    st.cache_data.clear()
    st.rerun()

store_df = load_data("Store Master")
sku_df = load_data("SKU Master")
sales_df = load_data("Sales Data")
config_df = load_data("Config")

# 🔑 Load ALL admin emails from Config (row-wise)
admin_emails = []
if not config_df.empty and "Admin Emails" in config_df.columns:
    admin_emails = config_df["Admin Emails"].dropna().astype(str).tolist()

# ========== SESSION STATE SAFE INIT ==========
defaults = {"employee": "", "party": "", "store_name": "", "category": "", "sku": "", "soh": 0}
for k, v in defaults.items():
    st.session_state.setdefault(k, v)

# ========== LOGIN SYSTEM ==========
st.sidebar.title("🔑 Employee Login")

login_user = st.sidebar.text_input("Username")
login_pass = st.sidebar.text_input("Password", type="password")
login_btn = st.sidebar.button("Login")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False
    st.session_state.employee = ""

if login_btn:
    user_row = config_df[
        (config_df["Username"].astype(str) == str(login_user)) &
        (config_df["Password"].astype(str) == str(login_pass))
    ]
    if not user_row.empty:
        st.session_state.logged_in = True
        st.session_state.employee = user_row.iloc[0]["Employee Name"]
        st.success(f"✅ Logged in as {st.session_state.employee}")
    else:
        st.error("❌ Invalid Username or Password")

if not st.session_state.logged_in:
    st.warning("Please log in to continue.")
    st.stop()

# ========== CASCADING DROPDOWNS ==========
employee = st.session_state.employee
st.write(f"👤 Logged in Employee: **{employee}**")

party_list = []
if employee:
    party_list = sorted(store_df.loc[store_df["Employee Name"] == employee, "Party"].dropna().astype(str).unique())
party_list = ["-- Select --"] + party_list
party = st.selectbox("Select Party", party_list, index=0)
party = "" if party == "-- Select --" else party
st.session_state.party = party

store_list = []
if employee and party:
    store_list = sorted(
        store_df.loc[(store_df["Employee Name"] == employee) & (store_df["Party"] == party), "Store Name"]
        .dropna().astype(str).unique()
    )
store_list = ["-- Select --"] + store_list
store_name = st.selectbox("Select Store", store_list, index=0)
store_name = "" if store_name == "-- Select --" else store_name
st.session_state.store_name = store_name

# Pull store info
if employee and party and store_name:
    row = store_df[
        (store_df["Employee Name"] == employee) &
        (store_df["Party"] == party) &
        (store_df["Store Name"] == store_name)
    ]
    store_info = row.iloc[0].to_dict() if not row.empty else {"City": "", "Visit Frequency": "", "Visit Days": ""}
else:
    store_info = {"City": "", "Visit Frequency": "", "Visit Days": ""}

st.write(f"**City:** {store_info.get('City','')}")
st.write(f"**Visit Frequency:** {store_info.get('Visit Frequency','')} ({store_info.get('Visit Days','')})")

# Category -> SKU
categories = sorted(sku_df["Category"].dropna().astype(str).unique()) if not sku_df.empty else []
categories = ["-- Select --"] + categories
category = st.selectbox("Select Category", categories, index=0)
category = "" if category == "-- Select --" else category
st.session_state.category = category

filtered_skus = sku_df[sku_df["Category"].astype(str) == str(category)] if category else sku_df.iloc[0:0]
sku_options = sorted(filtered_skus["SKU"].dropna().astype(str).unique()) if not filtered_skus.empty else []
sku_options = ["-- Select --"] + sku_options
sku = st.selectbox("Select SKU", sku_options, index=0)
sku = "" if sku == "-- Select --" else sku
st.session_state.sku = sku

sku_row = {}
if not filtered_skus.empty and sku:
    rr = filtered_skus[filtered_skus["SKU"].astype(str) == str(sku)]
    if not rr.empty:
        sku_row = rr.iloc[0].to_dict()

# Stock on Hand
soh = st.number_input("Stock on Hand (SOH)", min_value=0, step=1, value=int(st.session_state.soh))
st.session_state.soh = int(soh)

# ----- Sales lookup -----
sales_match = sales_df[
    (sales_df["Party"].astype(str) == str(party)) &
    (sales_df["Store Name"].astype(str) == str(store_name)) &
    (sales_df["City"].astype(str) == str(store_info.get("City",""))) &
    (sales_df["SKU"].astype(str) == str(sku))
] if party and store_name and sku else sales_df.iloc[0:0]

if not sales_match.empty:
    s = sales_match.iloc[0].to_dict()
    lm_net  = to_num(s.get("Last Month Net Sales", 0))
    lm_rtv  = to_num(s.get("Last Month RTV", 0))
    mtd_net = to_num(s.get("Running Month Net Sales", 0))
    mtd_rtv = to_num(s.get("Running Month RTV", 0))
else:
    lm_net = lm_rtv = mtd_net = mtd_rtv = 0

# ----- Suggested qty -----
today = datetime.today()
prev_year = today.year if today.month > 1 else today.year - 1
prev_month = today.month - 1 if today.month > 1 else 12
days_last_month = monthrange(prev_year, prev_month)[1]
total_days = days_last_month + today.day

total_sales = lm_net + mtd_net
avg_daily_offtake = 0
if total_days > 0:
    avg_daily_offtake = total_sales / total_days

# --- Dynamic reorder days based on Visit Frequency ---
visit_freq = to_num(store_info.get("Visit Frequency", 0))
if 1 <= visit_freq <= 6:
    reorder_days = 7
elif visit_freq >= 8:
    reorder_days = monthrange(today.year, today.month)[1]  # days in current month
else:
    reorder_days = 7  # default

if visit_freq > 0:
    reference_sales = int(round((avg_daily_offtake * reorder_days) / visit_freq))
else:
    reference_sales = int(round(avg_daily_offtake * reorder_days))

recommended_qty = max(reference_sales - to_num(soh), 0)

st.info(
    f"💡 Suggested Order Qty: {recommended_qty}  \n"
    f"(LM Sales={lm_net}, MTD Sales={mtd_net}, "
    f"Avg Daily Offtake={avg_daily_offtake:.2f}, Ref Demand={reference_sales}, "
    f"Reorder Days={reorder_days}, Visit Freq={visit_freq})"
)

# ========== ORDER FORM ==========
with st.form("order_form"):
    order_date = st.date_input("Order Date", datetime.today())
    remarks = st.text_area("Remarks")
    qty = st.number_input("Order Qty", min_value=0, step=1, value=0)
    submitted = st.form_submit_button("Submit Order")

if submitted:
    if not (employee and party and store_name and category and sku):
        st.error("Please select Employee, Party, Store, Category and SKU before submitting.")
    else:
        flag = "OK"
        if to_num(qty) > 1.2 * max(reference_sales, 1):
            flag = "Excess Order"
            if admin_emails:
                subject = f"⚠️ Trade Excess Order Alert - {employee}"
                body = f"""
Employee: {employee}
Store: {store_name} ({party}, {store_info.get('City','')})
SKU: {sku}
Ordered QTY: {qty}
Reference Sales (Offtake): {reference_sales}
Flag: {flag}
Remarks From Employee:
{remarks if remarks else "no remarks provided"}
"""
            # join all valid emails with comma
            recipients = ",".join([e.strip()for e in admin_emails if e and str(e).strip() !=""])
            send_email(recipients, subject, body)

        order_dict = {
            "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "Order Date": str(order_date),
            "Employee Name": employee,
            "Party": party,
            "Store Name": store_name,
            "City": store_info.get("City",""),
            "Category": category,
            "SKU": sku,
            "Qty": to_num(qty),
            "SOH": to_num(soh),
            "Remarks": remarks,
            "Last Month Net Sales": lm_net,
            "Running Month Net Sales": mtd_net,
            "Flag": flag
        }

        append_row("Orders", order_dict)
        st.success("✅ Order submitted successfully! (Admin alerted if excess)")
