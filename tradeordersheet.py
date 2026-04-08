import streamlit as st
import pandas as pd
import gspread
from google.oauth2.service_account import Credentials
from datetime import datetime
from calendar import monthrange
import uuid
import random
import string
import pickle
from google.auth.transport.requests import Request
from email.mime.text import MIMEText
import base64
from google_auth_oauthlib.flow import InstalledAppFlow
from googleapiclient.discovery import build
from streamlit_js_eval import get_geolocation
from googleapiclient.discovery import build
from googleapiclient.http import MediaFileUpload

# ========== CONFIG ==========
GOOGLE_SHEET_ID = st.secrets["google_sheet_id"]
DRIVE_FOLDER_ID = st.secrets["drive_folder_id"]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive"
]
# --- Google Sheets Auth (Service Account)
creds = Credentials.from_service_account_info(
    st.secrets["gcp_service_account"], scopes=SCOPES
)
client = gspread.authorize(creds)
sheet = client.open_by_key(GOOGLE_SHEET_ID)

# --- Gmail Auth ---
def get_gmail_service():
    creds = None
    # Load token from Streamlit secrets
    if "token" in st.secrets and "pickle_b64" in st.secrets["token"]:
        token_bytes = base64.b64decode(st.secrets["token"]["pickle_b64"])
        creds = pickle.loads(token_bytes)

    # If no valid creds, refresh or login
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

            # Save token locally (optional)
            with open("token.pickle", "wb") as token:
                pickle.dump(creds, token)

    return build("gmail", "v1", credentials=creds)


@st.cache_resource
def get_gmail_service_cached():
    return get_gmail_service()


gmail_service = get_gmail_service_cached()

# ========== HELPERS ==========
@st.cache_data(ttl=300)
def load_data(tab):
    try:
        ws = sheet.worksheet(tab)
        data = ws.get_all_values()

        if not data or len(data) < 2:
            return pd.DataFrame()

        return pd.DataFrame(data[1:], columns=data[0])

    except Exception as e:
        st.warning(f"{tab} load failed: {e}")
        return pd.DataFrame()

def append_row(tab, data_dict):
    ws = sheet.worksheet(tab)
    headers = ws.row_values(1)
    row = [data_dict.get(col, "") for col in headers]
    ws.append_row(row)


def to_num(x):
    try:
        return int(float(x))
    except:
        return 0
    
def send_email(service, to, subject, body):
    message = MIMEText(body)
    message['to'] = to
    message['subject'] = subject

    raw = base64.urlsafe_b64encode(message.as_bytes()).decode()
    service.users().messages().send(
        userId="me",
        body={'raw': raw}
    ).execute() 

# ========== LOGIN ==========
config_df = load_data("Config")
city_email_map = {}

if "City" in config_df.columns and "Emails" in config_df.columns:
    for _, row in config_df.iterrows():
        city_name = str(row["City"]).strip().lower()
        emails = str(row["Emails"]).split(",")

        # साफ emails
        emails = [e.strip() for e in emails if e.strip()]

        if city_name:
            city_email_map[city_name] = emails
st.sidebar.title("Login")

if "logged_in" not in st.session_state:
    st.session_state.logged_in = False

if not st.session_state.logged_in:
    user = st.sidebar.text_input("Username")
    password = st.sidebar.text_input("Password", type="password")

    if st.sidebar.button("Login"):
        row = config_df[(config_df["Username"] == user) & (config_df["Password"] == password)]
        if not row.empty:
            st.session_state.logged_in = True
            st.session_state.employee = row.iloc[0]["Employee Name"]
            st.session_state.role = row.iloc[0]["Role"]
            st.rerun()
        else:
            st.error("Invalid login")

    st.stop()

if st.sidebar.button("Logout"):
    st.session_state.clear()
    st.rerun()

employee = st.session_state.employee
role = st.session_state.role

# ========== ATTENDANCE ==========
if "attendance_done" not in st.session_state:
    st.session_state.attendance_done = False

if not st.session_state.attendance_done:
    st.subheader("📸 Attendance")
    photo = st.camera_input("Capture Photo")
    loc = get_geolocation()

    if st.button("Mark Attendance"):
        if photo and loc:
            ts = datetime.now().strftime("%Y%m%d_%H%M%S")
            file_name = f"{employee}_{ts}.jpg"

            with open(file_name, "wb") as f:
                f.write(photo.getbuffer())

            drive_service = build("drive", "v3", credentials=creds)
            media = MediaFileUpload(file_name, mimetype='image/jpeg')

            file = drive_service.files().create(
                body={'name': file_name, 'parents': [DRIVE_FOLDER_ID]},
                media_body=media,
                fields='id',
                supportsAllDrives=True
            ).execute()

            lat = loc['coords']['latitude']
            lon = loc['coords']['longitude']
            map_url = f"https://www.google.com/maps?q={lat},{lon}"

            append_row("Attendance", {
                "Employee": employee,
                "Time": ts,
                "Lat": lat,
                "Lon": lon,
                "Location Link": map_url,
                "Photo": file_name
            })

            st.session_state.attendance_done = True
            st.success("Attendance marked")
        else:
            st.error("Photo & location required")

    st.stop()

# ========== LOAD DATA ==========
store_df = load_data("Store Master")
sku_df = load_data("SKU Master")
sales_df = load_data("Sales Data")
orders_df = load_data("Orders")
target_df = load_data("Targets")

# ========== VISIT DAY FIX ==========
today_day_full = datetime.today().strftime("%A")

weekday_map = {
    "mon": "Monday", "monday": "Monday",
    "tue": "Tuesday", "tuesday": "Tuesday",
    "wed": "Wednesday", "wednesday": "Wednesday",
    "thu": "Thursday", "thur": "Thursday", "thursday": "Thursday",
    "fri": "Friday", "friday": "Friday",
    "sat": "Saturday", "saturday": "Saturday",
    "sun": "Sunday", "sunday": "Sunday"
}

filtered_store_df = store_df[
    store_df["Employee Name"].astype(str).str.contains(employee, case=False, na=False)
]

today_full = datetime.today().strftime("%A").lower()   # monday
today_short = datetime.today().strftime("%a").lower()  # mon

valid_rows = []

for _, row in filtered_store_df.iterrows():
    raw_days = str(row.get("Visit Days", "")).lower()

    # split safely
    days_list = [d.strip() for d in raw_days.split(",") if d.strip()]

    match_found = False

    for d in days_list:
        if d.startswith(today_short) or d.startswith(today_full):
            match_found = True
            break

    if match_found:
        valid_rows.append(row)

filtered_store_df = pd.DataFrame(valid_rows) if valid_rows else pd.DataFrame(columns=store_df.columns)

# ========== CASCADING ==========
party_list = sorted(filtered_store_df["Party"].dropna().unique())
party_list = ["-- Select --"] + party_list
party = st.selectbox("Party", party_list)
party = "" if party == "-- Select --" else party

store_list = []
if party:
    store_list = sorted(filtered_store_df[filtered_store_df["Party"] == party]["Store Name"].dropna().unique())

store_list = ["-- Select --"] + store_list
store_name = st.selectbox(f"Store (Today: {today_day_full})", store_list)
store_name = "" if store_name == "-- Select --" else store_name

store_row = filtered_store_df[filtered_store_df["Store Name"] == store_name]
store_info = store_row.iloc[0].to_dict() if not store_row.empty else {}
# ===== SHOW BEAT + VISIT FREQ =====
if store_name:
    st.write(f"📅 Visit Days: {store_info.get('Visit Days','')}")
    st.write(f"🔁 Visit Frequency: {store_info.get('Visit Frequency','')}")

# ========== CATEGORY FILTER ==========
categories = ["All"] + sorted(sku_df["Category"].dropna().unique().tolist())
selected_cat = st.selectbox("Filter Category", categories)

if selected_cat != "All":
    sku_df = sku_df[sku_df["Category"] == selected_cat]

# ========== PRODUCT SEARCH ==========
search_term = st.text_input("🔍 Search SKU")

if search_term:
    sku_df = sku_df[sku_df["SKU"].astype(str).str.contains(search_term, case=False, na=False)]

# ========== ORDER ENTRY ==========
st.subheader("📦 Order Entry")

cart = {}
today = datetime.today().strftime("%Y-%m-%d")
days_in_month = monthrange(datetime.today().year, datetime.today().month)[1]
visit_freq = to_num(store_info.get("Visit Frequency", 0))
city = str(store_info.get("City", "")).strip().lower()

h1,h2,h3,h4 = st.columns([3,2,2,2])
h1.markdown("**SKU**")
h2.markdown("**SOH**")
h3.markdown("**Suggested Qty**")
h4.markdown("**Order Qty**")

sku_df = sku_df.head(100)

for _, row in sku_df.iterrows():
    sku = row["SKU"]
    category = row.get("Category", "")

    sales_match = sales_df[
        (sales_df["Store Name"].astype(str) == str(store_name)) &
        (sales_df["SKU"].astype(str) == str(sku))
    ]

    lm_net = to_num(sales_match.iloc[0].get("Last 2 Month Avg Net Sales", 0)) if not sales_match.empty else 0
    daily = lm_net / days_in_month if days_in_month else 0

    if 1 <= visit_freq <= 6:
        reorder_days = 7
    elif visit_freq >= 8:
        reorder_days = days_in_month
    else:
        reorder_days = 7

    ref_sales = int(round((daily * reorder_days) / visit_freq)) if visit_freq else int(round(daily * reorder_days))

    c1,c2,c3,c4 = st.columns([3,2,2,2])
    c1.write(sku)

    soh = c2.number_input("SOH", min_value=0, key=f"soh_{sku}", label_visibility="collapsed")
    suggested = max(ref_sales - soh, 0)
    c3.markdown(f"<div style='text-align:center; color:green'>{suggested}</div>", unsafe_allow_html=True)

    qty = c4.number_input("Qty", min_value=0, key=f"qty_{sku}", label_visibility="collapsed")

    if qty > 0:
        cart[sku] = {
            "SKU": sku,
            "Category": category,
            "Qty": qty,
            "SOH": soh,
            "Suggested": suggested,
            "LM": lm_net,
            "MRP": float(row.get("MRP", 0) or 0)
        }

Remarks = st.text_area("Remarks")

# ========== FIXED SUBMIT ==========
if st.button("Submit Order", use_container_width=True):

    if not party or not store_name:
        st.error("Select store/party")
        st.stop()

    if not cart:
        st.warning("No items selected")
        st.stop()

    # ===== ORDER ID LOGIC =====
    existing_orders = orders_df.copy()

    if not existing_orders.empty and "Order ID" in existing_orders.columns:
        numeric_ids = existing_orders["Order ID"].astype(str).str.extract(r'ORD-(\d+)')[0]
        last_id = pd.to_numeric(numeric_ids, errors='coerce').max()
        next_order_id = int(last_id) + 1 if pd.notna(last_id) else 1
    else:
        next_order_id = 1

    suffix = ''.join(random.choices(string.ascii_uppercase + string.digits, k=2))
    order_id = f"ORD-{str(next_order_id).zfill(3)}-{suffix}"

    # ===== LOCATION =====
    loc = get_geolocation()
    lat = loc['coords']['latitude'] if loc and 'coords' in loc else ""
    lon = loc['coords']['longitude'] if loc and 'coords' in loc else ""
    map_url = f"https://www.google.com/maps?q={lat},{lon}" if lat and lon else ""

    # ===== LOOP =====
    ws = sheet.worksheet("Orders")
    headers = ws.row_values(1)
    rows_to_add = []

    for entry in cart.values():
     flag = "Excess Order" if entry["Qty"] > 1.25 * max(entry["Suggested"], 1) else "OK"
     data_dict = {
        "Timestamp": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Order Date": today,
        "Employee Name": employee,
        "Party": party,
        "Store Name": store_name,
        "City": city,
        "Category": entry["Category"],
        "SKU": entry["SKU"],
        "Qty": entry["Qty"],
        "SOH": entry["SOH"],
        "Remarks": Remarks,
        "Last 2 Month Avg Net Sales": entry["LM"],
        "Running Month Net Sales": 0,
        "Flag": flag,
        "Order ID": order_id,
        "Latitude": lat,
        "Longitude": lon,
        "Location Link": map_url
    }

    row = [data_dict.get(col, "") for col in headers]
    rows_to_add.append(row)
     # ✅ MUST BE INSIDE LOOP
    if flag == "Excess Order":
     to_emails = city_email_map.get(city, ["dhruvsinh@gmail.com"])
     send_email(
                gmail_service,
                to=", ".join(to_emails),
                subject="Excess Order Alert",
                body=f"""
        Order ID: {order_id}
        Employee: {employee}
        City: {city}
        Store: {store_name}
        SKU: {entry['SKU']}
        Qty: {entry['Qty']}
        Suggested: {entry['Suggested']}
        Remarks: {Remarks}
        """
            )
        # ✅ SINGLE API CALL (IMPORTANT)
    ws.append_rows(rows_to_add)

    st.success("Order Submitted")
    st.cache_data.clear()
    st.stop()

# ========== TODAY + MTD ==========
today = datetime.today().strftime("%Y-%m-%d")
current_month = datetime.today().strftime("%Y-%m")

if orders_df.empty:
    st.warning("No order data available")
    st.stop()

today_orders = orders_df[
    (orders_df["Employee Name"] == employee) &
    (orders_df["Order Date"] == today)
]

mtd_orders = orders_df[
    (orders_df["Employee Name"] == employee) &
    (orders_df["Order Date"].astype(str).str.startswith(current_month))
]

# ===== TODAY TABLE =====
st.subheader("📊 Today Orders")

cols = ["Order Date","Employee Name","Party","Store Name","City","SKU","Qty","SOH","Last 2 Month Avg Net Sales"]
cols = [c for c in cols if c in today_orders.columns]

st.dataframe(today_orders[cols])

# ===== MTD METRICS =====
st.subheader("📈 MTD Performance")
if not mtd_orders.empty:
    mtd_orders["Qty"] = pd.to_numeric(mtd_orders["Qty"], errors="coerce").fillna(0)

ach_qty = mtd_orders["Qty"].sum() if not mtd_orders.empty else 0

if not mtd_orders.empty:
   mtd_orders["Value"] = mtd_orders["Qty"] * mtd_orders["SKU"].map(sku_mrp)
ach_val = mtd_orders["Value"].sum() if not mtd_orders.empty else 0

tgt_qty = target_df[target_df["Employee"]==employee]["Target Qty"].astype(float).sum() if "Target Qty" in target_df.columns else 0
tgt_val = target_df[target_df["Employee"]==employee]["Target Value"].astype(float).sum() if "Target Value" in target_df.columns else 0

col1,col2,col3,col4 = st.columns(4)
col1.metric("MTD Qty", int(ach_qty))
col2.metric("Target Qty", int(tgt_qty))
col3.metric("MTD ₹", int(ach_val))
col4.metric("Target ₹", int(tgt_val))

# ========== ADMIN ==========
if role == "Admin":
    d = st.date_input("Select Date")
    rep = orders_df[orders_df["Order Date"] == d.strftime("%Y-%m-%d")]
    st.download_button("Download", rep.to_csv(index=False).encode(), file_name="report.csv")

st.success("System Ready ✅")
