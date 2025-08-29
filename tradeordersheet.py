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
import pickle
from calendar import monthrange

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


# --- Gmail Auth ---
def get_gmail_service():
    creds = None
    if "token" in st.secrets and "pickle_b64" in st.secrets["token"]:
        token_bytes = base64.b64decode(st.secrets["token"]["pickle_b64"])
        creds = pickle.loads(token_bytes)

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

        with open("token.pickle", "wb") as token:
            pickle.dump(creds, token)

    return build("gmail", "v1", credentials=creds)


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
        "Last 2 Month Avg Net Sales", "Running Month Net Sales", "Flag"
    ]
    if not headers:
        ws.insert_row(full_headers, 1)
        headers = full_headers
    row = [data_dict.get(col, "") for col in headers]
    ws.append_row(row)


def send_email(to, subject, body):
    try:
        if not to or str(to).strip() == "":
            return
        message = MIMEText(body)
        message['to'] = to
        message['subject'] = subject
        create_message = {'raw': base64.urlsafe_b64encode(message.as_bytes()).decode()}
        gmail_service.users().messages().send(userId="me", body=create_message).execute()
    except Exception as e:
        st.warning(f"⚠️ Email send failed (order saved anyway): {e}")


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

admin_emails = []
if not config_df.empty and "Admin Emails" in config_df.columns:
    admin_emails = config_df["Admin Emails"].dropna().astype(str).tolist()

defaults = {"employee": "", "party": "", "store_name": "", "category": ""}
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

# --- Store filter by weekday ---
today_weekday = datetime.today().strftime("%A")  # e.g. "Monday"
weekday_map = {"Mon": "Monday", "Tue": "Tuesday", "Wed": "Wednesday",
               "Thu": "Thursday", "Thur": "Thursday", "Fri": "Friday",
               "Sat": "Saturday", "Sun": "Sunday"}

store_list = []
if employee and party:
    filtered_stores = store_df[
        (store_df["Employee Name"] == employee) &
        (store_df["Party"] == party)
    ]
    valid_stores = []
    for _, row in filtered_stores.iterrows():
        visit_days_raw = str(row.get("Visit Days", ""))  # e.g. "Mon, Wed, Fri"
        visit_days = []
        for d in visit_days_raw.split(","):
            d = d.strip().title()
            if d in weekday_map:
                visit_days.append(weekday_map[d])
            elif d in weekday_map.values():
                visit_days.append(d)
        if today_weekday in visit_days:
            valid_stores.append(row["Store Name"])
    store_list = sorted(set(valid_stores))

store_list = ["-- Select --"] + store_list
store_name = st.selectbox(f"Select Store (Today: {today_weekday})", store_list, index=0)
store_name = "" if store_name == "-- Select --" else store_name
st.session_state.store_name = store_name

# Store info
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

# Category dropdown
categories = sorted(sku_df["Category"].dropna().astype(str).unique()) if not sku_df.empty else []
categories = ["-- Select --"] + categories
category = st.selectbox("Select Category", categories, index=0)
category = "" if category == "-- Select --" else category
st.session_state.category = category

# ======== PRODUCT MATRIX (LIVE UPDATES) ========
if category:
    st.subheader("📋 Order Entry")

    filtered_skus = sku_df[sku_df["Category"].astype(str) == str(category)] if category else sku_df.iloc[0:0]

    if not filtered_skus.empty:
        # Table header
        header_cols = st.columns([3, 2, 2, 2, 2, 2])
        header_cols[0].markdown("**SKU**")
        header_cols[1].markdown("**Last 2M Avg Sales**")
        header_cols[2].markdown("**Daily Offtake**")
        header_cols[3].markdown("**SOH**")
        header_cols[4].markdown("**Suggested Qty**")
        header_cols[5].markdown("**Order Qty**")

        order_entries = []
        today = datetime.today()
        days_in_current_month = monthrange(today.year, today.month)[1]

        for idx, row in filtered_skus.iterrows():
            sku = row["SKU"]

            sales_match = sales_df[
                (sales_df["Party"].astype(str) == str(party)) &
                (sales_df["Store Name"].astype(str) == str(store_name)) &
                (sales_df["City"].astype(str) == str(store_info.get("City",""))) &
                (sales_df["SKU"].astype(str) == str(sku))
            ]

            if not sales_match.empty:
                row_dict = sales_match.iloc[0].to_dict()
                lm_net = to_num(row_dict.get("Last 2 Month Avg Net Sales", row_dict.get("Last Month Net Sales", 0)))
            else:
                lm_net = 0

            daily_offtake = lm_net / days_in_current_month if days_in_current_month > 0 else 0

            visit_freq = to_num(store_info.get("Visit Frequency", 0))
            if 1 <= visit_freq <= 6:
                reorder_days = 7
            elif visit_freq >= 8:
                reorder_days = days_in_current_month
            else:
                reorder_days = 7

            if visit_freq > 0:
                ref_sales = int(round((daily_offtake * reorder_days) / visit_freq))
            else:
                ref_sales = int(round(daily_offtake * reorder_days))

            # --- Row Layout ---
            row_cols = st.columns([3, 2, 2, 2, 2, 2])
            row_cols[0].write(sku)
            row_cols[1].write(lm_net)
            row_cols[2].write(f"{daily_offtake:.2f}")

            soh_val = row_cols[3].number_input("", min_value=0, step=1, key=f"soh_{sku}")
            suggested = max(ref_sales - soh_val, 0)   # ✅ updates live
            row_cols[4].write(suggested)

            qty_val = row_cols[5].number_input("", min_value=0, step=1, key=f"qty_{sku}")

            order_entries.append({
                "SKU": sku,
                "Qty": qty_val,
                "SOH": soh_val,
                "Suggested": suggested,
                "LM Sales": lm_net
            })

        remarks = st.text_area("Remarks")
        submitted = st.button("Submit Order")   # ✅ normal button

        if submitted:
            for entry in order_entries:
                if entry["SOH"] == 0 and entry["Qty"] == 0:
                    continue

                flag = "Excess Order" if entry["Qty"] > 1.2 * max(entry["Suggested"], 1) else "OK"

                order_dict = {
                    "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                    "Order Date": today.strftime("%Y-%m-%d"),
                    "Employee Name": employee,
                    "Party": party,
                    "Store Name": store_name,
                    "City": store_info.get("City",""),
                    "Category": category,
                    "SKU": entry["SKU"],
                    "Qty": entry["Qty"],
                    "SOH": entry["SOH"],
                    "Remarks": remarks,
                    "Last 2 Month Avg Net Sales": entry["LM Sales"],
                    "Running Month Net Sales": 0,
                    "Flag": flag
                }

                append_row("Orders", order_dict)

                if flag == "Excess Order" and admin_emails:
                    subject = f"⚠️ Trade Excess Order Alert - {employee}"
                    body = f"""
Employee: {employee}
Store: {store_name} ({party}, {store_info.get('City','')})
SKU: {entry["SKU"]}
Ordered QTY: {entry["Qty"]}
Reference Sales (Offtake): {entry["Suggested"]}
Flag: {flag}
Remarks From Employee:
{remarks if remarks else "no remarks provided"}
"""
                    recipients = ",".join([e.strip() for e in admin_emails if e and str(e).strip() != ""])
                    send_email(recipients, subject, body)

            st.success("✅ Orders submitted successfully!")
