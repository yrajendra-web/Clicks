print("🔥 YES THIS FILE IS RUNNING (COMBINED MODULE)")

import os
import sys
import smtplib
import numpy as np
import pandas as pd
import gspread
from datetime import datetime, timedelta, timezone
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from pymongo import MongoClient
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from google.oauth2.service_account import Credentials
from gspread.exceptions import WorksheetNotFound
from dotenv import load_dotenv

# =========================
# ENV & CONFIG
# =========================
load_dotenv()

# Google Sheets Config
CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS", "credentials.json")
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# SMTP Config
SMTP_HOST = os.getenv("SMTP_HOST", "smtp.gmail.com")
SMTP_PORT = int(os.getenv("SMTP_PORT", 465))
SMTP_USER = os.getenv("SMTP_USER", "yrajendra.adi@gmail.com")
SMTP_PASS = os.getenv("SMTP_PASS") 

# Mongo Config
LOCAL_MONGO_URI = os.getenv("LOCAL_MONGO_URI")

# =========================
# UTILITIES & HELPERS
# =========================
def clean_domain(d: str) -> str:
    return (d or "").replace("https://", "").replace("http://", "").strip()

def to_int(v) -> int:
    try:
        return int(v)
    except (ValueError, TypeError):
        return 0

def normalize_type(t) -> str:
    return str(t or "").strip().lower()

def is_table_domain(domain_type: str) -> bool:
    return normalize_type(domain_type) in {
        "programmatic",
        "direct apply",
        "direct_apply",
        "direct-apply",
    }

def get_or_create_worksheet(sheet, name, rows=200, cols=30):
    try:
        return sheet.worksheet(name)
    except WorksheetNotFound:
        return sheet.add_worksheet(title=name, rows=rows, cols=cols)

def make_unique_columns(columns):
    seen, fixed = {}, []
    for c in columns:
        if c not in seen:
            seen[c] = 1
            fixed.append(c)
        else:
            seen[c] += 1
            fixed.append(f"{c}_{seen[c]}")
    return fixed

# =========================
# SAFE SHEET UPDATE
# =========================
def smart_update(ws, df, key_col="Date"):
    try:
        old = pd.DataFrame(ws.get_all_records())
    except Exception:
        old = pd.DataFrame()

    # Clean NaN / None
    df = df.replace([np.nan, None], "")
    df = df.fillna("")
    df.columns = make_unique_columns(df.columns)

    if old.empty:
        ws.clear()
        ws.update([df.columns.tolist()] + df.astype(str).values.tolist())
        return

    all_cols = list(dict.fromkeys(list(old.columns) + list(df.columns)))
    old = old.reindex(columns=all_cols, fill_value="")
    df = df.reindex(columns=all_cols, fill_value="")

    keys = set(df[key_col]) if key_col in df.columns else set()
    if key_col in old.columns:
        old = old[~old[key_col].astype(str).isin(keys)]

    merged = pd.concat([old, df], ignore_index=True).fillna("")
    ws.clear()
    ws.update([merged.columns.tolist()] + merged.astype(str).values.tolist())

# =========================
# MONGO CONNECTION
# =========================
def connect_mongo(domain_record, db_name):
    mongo_uri = domain_record.get("mongo_uri")
    if not mongo_uri:
        raise ValueError(f"No mongo_uri found for {db_name}")
    client = MongoClient(mongo_uri, serverSelectionTimeoutMS=8000)
    client.admin.command("ping")
    return client[db_name], db_name

# =========================
# FETCHERS: GOOGLE SHEETS
# =========================
def fetch_postings_programmatic(domain_name, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"employerId": {"$exists": True}},
        {"job_status": {"$ne": "3.0"}},
        {"gpost": 5.0},
        {"gpost_date": {"$gte": min_dt, "$lt": max_dt}},
    ]
    if employer_id:
        match_conditions.append({"employerId": employer_id})
    if exclude_list:
        match_conditions.append({"employerId": {"$nin": exclude_list}})  

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$group": {
            "_id": {
                "employerId": "$employerId",
                "gpost_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$gpost_date"}},
                "company": {"$ifNull": ["$company", "Unknown"]}
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.gpost_date", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "PostingsCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db[collection_name].aggregate(pipeline, allowDiskUse=True))

def fetch_expires_programmatic(domain_name, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"employerId": {"$exists": True}},
        {"gpost_expire_date": {"$gte": min_dt, "$lt": max_dt}},
    ]
    if employer_id:
        match_conditions.append({"employerId": employer_id})
    if exclude_list:
        match_conditions.append({"employerId": {"$nin": exclude_list}})

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$group": {
            "_id": {
                "employerId": "$employerId",
                "expire_date": {"$dateToString": {"format": "%Y-%m-%d", "date": "$gpost_expire_date"}},
                "company": {"$ifNull": ["$company", "Unknown"]}
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.expire_date", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "ExpiresCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db[collection_name].aggregate(pipeline, allowDiskUse=True))  

def fetch_views_programmatic(domain_name, db, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"isBot": False},
        {"browserType": {"$ne": "Unknown"}},
        {"deviceType": {"$ne": "Unknown"}},
        {"isFromGoogle": True},
        {"createdDt": {"$gt": min_dt, "$lt": max_dt}},
    ]
    if employer_id:
        match_conditions.append({"employerId": employer_id})
    if exclude_list:
        match_conditions.append({"employerId": {"$nin": exclude_list}})  

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$project": {
            "company": {"$ifNull": ["$company", "Unknown"]},
            "viewDate": {"$substr": ["$createdDt", 0, 10]},
            "employerId": 1,
        }},
        {"$group": {
            "_id": {
                "company": "$company", "viewDate": "$viewDate", "employerId": "$employerId",
            },
            "count": {"$sum": 1},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.viewDate", "EmployerId": "$_id.employerId",
            "Company": "$_id.company", "ViewsCount": "$count",
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db["userAnalytics"].aggregate(pipeline, allowDiskUse=True))

def fetch_unique_ips(domain_name, db, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match_conditions = [
        {"isBot": False},
        {"browserType": {"$ne": "Unknown"}},
        {"deviceType": {"$ne": "Unknown"}},
        {"isFromGoogle": True},
        {"createdDt": {"$gt": min_dt, "$lt": max_dt}},
    ]
    if employer_id:
        match_conditions.append({"employerId": employer_id})
    if exclude_list:
        match_conditions.append({"employerId": {"$nin": exclude_list}})  

    pipeline = [
        {"$match": {"$and": match_conditions}},
        {"$project": {
            "sourceEmployerId": {"$ifNull": ["$extraInfo.sourceEmployerId", ""]},
            "employerId": 1,
            "company": {"$ifNull": ["$company", "Unknown"]},
            "date": {"$substr": ["$createdDt", 0, 10]},
            "ipAddress": 1
        }},
        {"$group": {
            "_id": {
                "sourceEmployerId": "$sourceEmployerId", "employerId": "$employerId",
                "company": "$company", "date": "$date"
            },
            "ips": {"$addToSet": "$ipAddress"}
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.date", "sourceEmployerId": "$_id.sourceEmployerId",
            "EmployerId": "$_id.employerId", "Company": "$_id.company",
            "UniqueIpCount": {"$size": "$ips"},
            "Domain": domain_name, "Domain Type": domain_type
        }},
    ]
    return list(db["userAnalytics"].aggregate(pipeline, allowDiskUse=True))

# =========================
# FETCHERS: EMAIL REPORTS
# =========================
def fetch_email_views(domain_name, db, employer_id, exclude_list, min_dt, max_dt, domain_type):
    match = {
        "isBot": False,
        "browserType": {"$ne": "Unknown"},
        "deviceType": {"$ne": "Unknown"},
        "isFromGoogle": True,
        "createdDt": {"$gt": min_dt, "$lt": max_dt},
    }
    if employer_id:
        match["employerId"] = employer_id

    if exclude_list:
        match["employerId"] = match.get("employerId", {})
        if isinstance(match["employerId"], dict):
            match["employerId"]["$nin"] = exclude_list
        else:
            match["employerId"] = {
                "$in": [match["employerId"]],
                "$nin": exclude_list
            }

    pipeline = [
        {"$match": match},
        {"$project": {
            "Company": {"$ifNull": ["$company", {"$ifNull": ["$companyName", "Unknown"]}]},
            "date": {"$substr": ["$createdDt", 0, 10]},
            "extraInfo": 1,
            "ipAddress": 1,
        }},
        {"$group": {
            "_id": {
                "Company": "$Company",
                "End": {"$ifNull": ["$extraInfo.sourceEmployerId", ""]},
                "Date": "$date",
            },
            "ViewsCount": {"$sum": 1},
            "UniqueIpCount": {"$addToSet": "$ipAddress"},
        }},
        {"$project": {
            "_id": 0, "Date": "$_id.Date", "Company": "$_id.Company",
            "sourceEmployerId": "$_id.End", "ViewsCount": 1,
            "UniqueIpCount": {"$size": "$UniqueIpCount"},
            "Domain": domain_name, "Domain Type": domain_type,
        }},
    ]
    return list(db["userAnalytics"].aggregate(pipeline, allowDiskUse=True))

def fetch_email_postings(db, collection_name, min_dt, max_dt):
    match = {"gpost": 5, "gpost_date": {"$gte": min_dt, "$lt": max_dt}}
    return db[collection_name].count_documents(match)   

def fetch_email_expires(db, collection_name, min_dt, max_dt):
    match = {"gpost_expire_date": {"$gte": min_dt, "$lt": max_dt}}
    return db[collection_name].count_documents(match)   

# =========================
# WORKER
# =========================
def process_domain(row, min_dt, max_dt, domains_col):
    domain = row.get("Domain", "").strip()
    db_name = row.get("Database", "").strip()
    domain_type = row.get("Domain Type", "").strip()
    employer_id = str(row.get("EmployerId", "")).strip()
    collection_name = row.get("Collection", "").strip()
    
    exclude_raw = row.get("Exclude_Employers", "")
    exclude_list = [e.strip() for e in exclude_raw.split(",") if e.strip()]

    if not domain or not db_name:
        return {"success": False, "reason": "Missing domain or database"}

    record = domains_col.find_one({"domain": db_name})
    if not record:
        print(f"⚠️ Skipped (No Mongo record): {db_name}")
        return {"success": False, "reason": "No Mongo record"}

    try:
        db, _ = connect_mongo(record, db_name)

        # 1) Sheets Pipeline
        postings = fetch_postings_programmatic(domain, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type)
        views = fetch_views_programmatic(domain, db, employer_id, exclude_list, min_dt, max_dt, domain_type)
        unique = fetch_unique_ips(domain, db, employer_id, exclude_list, min_dt, max_dt, domain_type)
        expires = fetch_expires_programmatic(domain, db, collection_name, employer_id, exclude_list, min_dt, max_dt, domain_type)

        # 2) Email Pipeline
        email_views = fetch_email_views(domain, db, employer_id, exclude_list, min_dt, max_dt, domain_type)
        email_postings = fetch_email_postings(db, collection_name, min_dt, max_dt)
        email_expires = fetch_email_expires(db, collection_name, min_dt, max_dt)

        print(f"✅ {domain} done. (S-Views: {len(views)}, E-Views: {len(email_views)})")
        return {
            "success": True,
            "domain": domain,
            "sheet_data": (postings, views, unique, expires),
            "email_data": {
                "views": email_views,
                "postings": email_postings,
                "expires": email_expires
            }
        }
    except Exception as e:
        print(f"❌ {domain} failed during fetch: {e}")
        return {"success": False, "reason": str(e)}

# ==========================================================
# HTML BUILDER (EMAIL)
# ==========================================================
def build_html(name, email_address, domains, report_date):
    total_domains = len(domains)
    total_clicks = sum(d["clicks"] for d in domains.values())
    total_ips = sum(d["ips"] for d in domains.values())
    companies = {c for d in domains.values() for (c, _) in d["rows"].keys()}

    blocks = ""
    for domain, d in sorted(domains.items(), key=lambda x: x[1]["clicks"], reverse=True):
        domain_type = d.get("type", "") or ""
        table = ""
        company_line = " &nbsp;&nbsp; ".join(
            [f"{k}: {v}" for k, v in d.get("company_summary", {}).items()]
        )

        if is_table_domain(domain_type):
            rows_html = ""
            row_items = sorted(d["rows"].items(), key=lambda kv: kv[1]["clicks"], reverse=True)
            for (company, source_employer_id), s in row_items:
                if not source_employer_id:
                    continue
                rows_html += f"""
    <tr>
    <td style="padding: 10px;">{company}</td>
    <td style="padding: 10px;">{source_employer_id}</td>
    <td style="padding: 10px; text-align: right;">{s['clicks']}</td>
    <td style="padding: 10px; text-align: right;">{s['ips']}</td>
    </tr>
"""
            table = f"""
    <table style="width: 100%; border-collapse: collapse; margin-top: 18px; background: #fafbff; border-radius: 14px; overflow: hidden; border: 1px solid #e2e7ff;">
    <thead>
    <tr style="background: #eef2ff;">
    <th style="padding: 10px; font-size: 12px; text-align: left;">Company</th>
    <th style="padding: 10px; font-size: 12px; text-align: left;">extraInfo.sourceEmployerId</th>
    <th style="padding: 10px; font-size: 12px; text-align: right;">Clicks</th>
    <th style="padding: 10px; font-size: 12px; text-align: right;">Unique IPs</th>
    </tr>
    </thead>
    <tbody>
{rows_html}
    </tbody>
    </table>
"""
            blocks += f"""
    <div style="padding: 26px; margin-bottom: 28px; border-radius: 24px; background: #ffffff; border: 1px solid #e4e6ef; box-shadow: 0 8px 24px rgba(99,102,241,0.14);">
    <h3 style="margin: 0 0 6px 0; font-size: 20px; color: #4f46e5; font-weight: 600;">{clean_domain(domain)}</h3>
    <div style="font-size: 14px; color: #374151; margin-bottom: 14px;"><strong>Domain Type:</strong>
    <span style="display: inline-block; padding: 3px 10px; border-radius: 999px; background: #eef2ff; color: #4338ca; font-size: 12px; font-weight: 600;"> {domain_type} </span></div>
    <div style="margin-top: 10px; font-size: 15px;">
    <strong>Total Clicks:</strong> {d.get('clicks', 0)} 
    &nbsp;&nbsp; 
    <strong>Unique IPs:</strong> {d.get('ips', 0)}
    &nbsp;&nbsp; 
    <strong>Total Postings:</strong> {d.get('postings', 0)}
    &nbsp;&nbsp; 
    <strong>Expires:</strong> {d.get('expires', 0)}
</div>
    <div style="margin-top:8px; font-size:14px; color:#111827;">
    [ {company_line} ]
</div>
{table}
    </div>
"""
        else:
            blocks += f"""
    <div style="padding: 26px; margin-bottom: 28px; border-radius: 24px; background: #ffffff; border: 1px solid #e4e6ef; box-shadow: 0 8px 24px rgba(99,102,241,0.14);">
    <h3 style="margin: 0 0 6px 0; font-size: 20px; color: #4f46e5; font-weight: 600;">{clean_domain(domain)}</h3>
    <div style="font-size: 14px; color: #374151; margin-bottom: 12px;"><strong>Domain Type:</strong> {domain_type}</div>
    <div style="font-size: 15px; line-height: 1.6;">
    <strong>Clicks:</strong> {d['clicks']}<br /> 
    <strong>Unique IPs:</strong> {d['ips']}<br />
    <strong>Total Postings:</strong> {d['postings']}
    </div>
    </div>
"""
    return f"""
    <p>&nbsp;</p>
    <div style="padding: 40px;">
    <div style="max-width: 840px; margin: 0 auto; background: linear-gradient(145deg,#ffffff,#f5f7ff); padding: 44px; border-radius: 28px; box-shadow: 0 14px 36px rgba(80,80,200,0.18); font-family: 'Inter', -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif;">
    <h1 style="margin: 0 0 8px 0; font-size: 28px; font-weight: 500; color: #111827; letter-spacing: -0.4px; line-height: 1.2;">Hey {name},</h1>
    <h3 style="margin: 0 0 26px 0; font-size: 17px; font-weight: 600; color: #6b7280; line-height: 1.5;">Here&rsquo;s your daily domain wise clicks report.</h3>
    <h1 style="margin: 0 0 10px 0; font-size: 30px; font-weight: 600; color: #4338ca; letter-spacing: -0.4px;">Daily Domain Wise Clicks Report ⚡</h1>
    <p style="margin: 0 0 30px 0; color: #6b7280; font-size: 15px;">Date: <strong>{report_date}</strong></p>
    <div style="background: linear-gradient(135deg,#e4e7ff,#d8dcff); padding: 22px 26px; border-radius: 22px; border-left: 6px solid #6366f1; margin-bottom: 36px;">
    <div style="font-size: 15px; color: #374151; line-height: 1.7;"><strong>🌐 Domains:</strong> {total_domains}<br /> <strong>🏢 Companies:</strong> {len(companies)}<br /> <strong>🖱 Total Clicks:</strong> {total_clicks:,}<br /> <strong>🧍 Unique IPs:</strong> {total_ips:,}</div>
    </div>
{blocks}
    <div style="margin-top: 42px; text-align: center; color: #9ca3af; font-size: 13px;">Sent to <strong>{name}</strong> &middot; {email_address}<br /> Generated automatically &middot; Daily Domain Analytics</div>
    </div>
    </div>
"""

# =========================
# MAIN EXECUTION
# =========================
def main():
    if len(sys.argv) < 2:
        print("❌ Usage: python script.py YYYY-MM-DD")
        sys.exit(1)

    date_str = sys.argv[1]
    min_dt = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    max_dt = min_dt + timedelta(days=1)

    print(f"🚀 Initializing unified report builder for {date_str}...")

    # Load local Mongo
    try:
        local_client = MongoClient(LOCAL_MONGO_URI)
        domains_col = local_client["mongo_creds"]["creds"]
        stats_col = local_client["daily_domain_stats"]["stats"]
        email_col = local_client["daily_domain_stats"]["email"]
    except Exception as e:
        print(f"❌ Failed to connect to local Mongo DB: {e}")
        sys.exit(1)

    # Clean old stats
    stats_col.delete_many({"Date": date_str})

    # Auth Google Sheets
    try:
        creds = Credentials.from_service_account_file(CREDENTIALS_FILE, scopes=SCOPES)
        gc = gspread.authorize(creds)
        sheet = gc.open("Domain Stats")
        domains_ws = sheet.worksheet("Domains")
        domains_rows = domains_ws.get_all_records()
        
        postings_ws = get_or_create_worksheet(sheet, "Postings")
        views_ws = get_or_create_worksheet(sheet, "Views")
        unique_ws = get_or_create_worksheet(sheet, "UniqueClicks")
        expires_ws = get_or_create_worksheet(sheet, "Expires")
    except Exception as e:
        print(f"❌ Failed to authorize or load Google Sheets: {e}")
        sys.exit(1)

    # Prepare Domain Data
    for r in domains_rows:
        r["Domain"] = (r.get("Domain") or "").strip()
        r["Database"] = (r.get("Database") or "").strip()
        r["Domain Type"] = (r.get("Domain Type") or "").strip()
        r["EmployerId"] = (str(r.get("EmployerId") or "")).strip()

    # Collectors
    all_sheet_postings, all_sheet_views, all_sheet_unique, all_sheet_expires = [], [], [], []
    email_all_rows = []
    domain_postings_map = {}
    domain_expires_map = {}

    # Thread Pool Data Fetching
    with ThreadPoolExecutor(max_workers=8) as ex:
        futures = {ex.submit(process_domain, row, min_dt, max_dt, domains_col): row for row in domains_rows}
        for fut in as_completed(futures):
            res = fut.result()
            if not res["success"]:
                continue
            
            # Aggregate Sheet Data
            p, v, u, e = res["sheet_data"]
            all_sheet_postings.extend(p)
            all_sheet_views.extend(v)
            all_sheet_unique.extend(u)
            all_sheet_expires.extend(e)

            # Aggregate Email Data
            email_data = res["email_data"]
            email_all_rows.extend(email_data["views"])
            domain_postings_map[res["domain"]] = email_data["postings"]
            domain_expires_map[res["domain"]] = email_data["expires"]

    print("POSTINGS MAP:", domain_postings_map)
    print("EXPIRES MAP:", domain_expires_map)

    # Write back to Local Mongo (Combined Data)
    combined_mongo_inserts = []
    now = datetime.now(timezone.utc)
    for item in all_sheet_postings + all_sheet_views + all_sheet_unique + email_all_rows:
        item["InsertedAt"] = now
        combined_mongo_inserts.append(item)

    if combined_mongo_inserts:
        try:
            stats_col.insert_many(combined_mongo_inserts)
            print("✅ Stats safely backed up to MongoDB.")
        except Exception as e:
            print(f"❌ Failed to insert into Mongo stats_col: {e}")

    # Write back to Google Sheets
    try:
        if all_sheet_postings: smart_update(postings_ws, pd.DataFrame(all_sheet_postings).fillna(""))
        if all_sheet_views: smart_update(views_ws, pd.DataFrame(all_sheet_views).fillna(""))
        if all_sheet_unique: smart_update(unique_ws, pd.DataFrame(all_sheet_unique).fillna(""))
        if all_sheet_expires: smart_update(expires_ws, pd.DataFrame(all_sheet_expires).fillna(""))
        print("✅ Google Sheets Smart Update Complete.")
    except Exception as e:
        print(f"❌ Failed to update Google Sheets: {e}")

    # =========================
    # EMAIL AGGREGATION & SEND
    # =========================
    print("🚀 Building and sending Email Reports...")
    domains_summary = defaultdict(lambda: {
        "type": "", "clicks": 0, "ips": 0, "postings": 0, "expires": 0,
        "rows": defaultdict(lambda: {"clicks": 0, "ips": 0})
    })

    # Pre-seed to guarantee zero-traffic domains show up
    for r in domains_rows:
        dom = r["Domain"]
        if dom:
            domains_summary[dom]["type"] = r["Domain Type"]

    # Aggregate
    for r in email_all_rows:
        dom = r.get("Domain", "")
        if not dom: continue

        d = domains_summary[dom]
        d["type"] = r.get("Domain Type", d["type"])
        
        clicks = to_int(r.get("ViewsCount", 0))
        ips = to_int(r.get("UniqueIpCount", 0))
        d["clicks"] += clicks
        d["ips"] += ips

        company = r.get("Company", "Unknown")
        source_employer_id = r.get("sourceEmployerId", "") or ""
        d["rows"][(company, source_employer_id)]["clicks"] += clicks
        d["rows"][(company, source_employer_id)]["ips"] += ips

    for dom, postings in domain_postings_map.items():
        if dom in domains_summary:
            domains_summary[dom]["postings"] = postings  

    for dom, exp in domain_expires_map.items():
        if dom in domains_summary:
            domains_summary[dom]["expires"] = exp  

    # Build Top 5 Company Summaries
    for dom, d in domains_summary.items():
        company_clicks = {}
        for (company, _), row_data in d["rows"].items():
            company_clicks[company] = company_clicks.get(company, 0) + row_data["clicks"]
        d["company_summary"] = dict(sorted(company_clicks.items(), key=lambda x: x[1], reverse=True)[:5]) 

    # Send Process
    try:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT)
        server.login(SMTP_USER, SMTP_PASS)
        
        emails_sent = 0
        for r in email_col.find({}, {"_id": 0}):
            to_email = r.get("email")
            if not to_email: continue

            msg = MIMEMultipart("alternative")
            msg["From"] = "Daily Clicks Report"
            msg["To"] = to_email
            msg["Subject"] = f"Daily Domain Click Report - {date_str}"
            html_content = build_html(r.get("Name", "There"), to_email, domains_summary, date_str)
            msg.attach(MIMEText(html_content, "html"))
            
            server.send_message(msg)
            emails_sent += 1

        server.quit()
        print(f"🚀 Email sent successfully to {emails_sent} recipients.")
    except Exception as e:
        print(f"❌ Failed to send emails: {e}")

    print("🎉 ALL DONE. Pipeline executed fully.")

if __name__ == "__main__":
    main()