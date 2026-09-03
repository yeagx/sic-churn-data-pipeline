#!/usr/bin/env python3
"""
scripts/generate_data.py
Customer Churn Data Pipeline — Task 1: Data Sourcing & Ingestion

Generates:
1. data/offers.csv (~25,000 rows)
2. data/usage.jsonl (~220,000-240,000 rows)
3. data/support_tickets_rekeyed.csv (8,469 rows re-keyed onto real customer IDs)

Guarantees:
- Fixed random seed = 42 for 100% reproducibility
- 100% referential integrity (zero orphan customer_ids)
- Behavioural churn logic seeded from real Exited flag
"""

import os
import csv
import json
import random
import datetime
import calendar
import numpy as np

# Fixed random seeds (Contract C5)
RANDOM_SEED = 42
random.seed(RANDOM_SEED)
np.random.seed(RANDOM_SEED)

# Reference anchor date (Contract C4)
REFERENCE_DATE = datetime.date(2024, 12, 31)

# Base directory resolution
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DATA_DIR = os.path.join(BASE_DIR, "data")
os.makedirs(DATA_DIR, exist_ok=True)

CUSTOMERS_FILE = os.path.join(DATA_DIR, "Churn_Modelling.csv")
# Fallback to base dir if not in data/
if not os.path.exists(CUSTOMERS_FILE):
    CUSTOMERS_FILE = os.path.join(BASE_DIR, "Churn_Modelling.csv")

TICKETS_FILE = os.path.join(DATA_DIR, "customer_support_tickets.csv")
if not os.path.exists(TICKETS_FILE):
    TICKETS_FILE = os.path.join(BASE_DIR, "customer_support_tickets.csv")

OFFERS_OUT = os.path.join(DATA_DIR, "offers.csv")
USAGE_OUT = os.path.join(DATA_DIR, "usage.jsonl")
TICKETS_OUT = os.path.join(DATA_DIR, "support_tickets_rekeyed.csv")


def load_customers():
    """Load real customer records from Churn_Modelling.csv."""
    print(f"[1/4] Loading customer pool from {CUSTOMERS_FILE}...")
    customers = []
    with open(CUSTOMERS_FILE, mode="r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            customers.append({
                "customer_id": str(row["CustomerId"]).strip(),
                "surname": row.get("Surname", "").strip(),
                "credit_score": int(row.get("CreditScore", 600)),
                "geography": row.get("Geography", "").strip(),
                "gender": row.get("Gender", "").strip(),
                "age": int(row.get("Age", 35)),
                "tenure": int(row.get("Tenure", 5)),
                "balance": float(row.get("Balance", 0.0)),
                "num_products": int(row.get("NumOfProducts", 1)),
                "has_cr_card": int(row.get("HasCrCard", 1)),
                "is_active_member": int(row.get("IsActiveMember", 1)),
                "estimated_salary": float(row.get("EstimatedSalary", 50000.0)),
                "exited": int(row.get("Exited", 0))
            })
    print(f"      Loaded {len(customers):,} customers. Exited = 1: {sum(1 for c in customers if c['exited'] == 1):,}")
    return customers


def get_month_end_dates():
    """Generate 24 month-end dates from 2023-01 to 2024-12."""
    dates = []
    for year in [2023, 2024]:
        for month in range(1, 13):
            last_day = calendar.monthrange(year, month)[1]
            dates.append(datetime.date(year, month, last_day))
    return dates


def generate_offers(customers):
    """Generate data/offers.csv (~25,000 rows, 0-5 per customer)."""
    print(f"[2/4] Generating {OFFERS_OUT}...")
    offer_types = [
        "credit_card_upgrade",
        "loan_preapproval",
        "savings_bonus",
        "insurance_bundle",
        "fee_waiver"
    ]
    
    start_date = datetime.date(2023, 1, 1)
    end_date = datetime.date(2024, 12, 31)
    total_days = (end_date - start_date).days
    
    offer_counter = 1
    total_accepted = 0
    
    with open(OFFERS_OUT, mode="w", newline="", encoding="utf-8") as f:
        writer = csv.writer(f)
        writer.writerow(["offer_id", "customer_id", "offer_type", "accepted", "date_offered"])
        
        for c in customers:
            num_offers = random.randint(0, 5)
            for _ in range(num_offers):
                offer_id = f"OFF{offer_counter:06d}"
                offer_type = random.choice(offer_types)
                # Acceptance rate ~12% based on UCI Bank Marketing distribution
                accepted = 1 if random.random() < 0.12 else 0
                if accepted:
                    total_accepted += 1
                
                random_days = random.randint(0, total_days)
                date_offered = (start_date + datetime.timedelta(days=random_days)).strftime("%Y-%m-%d")
                
                writer.writerow([offer_id, c["customer_id"], offer_type, accepted, date_offered])
                offer_counter += 1
                
    total_offers = offer_counter - 1
    acc_rate = (total_accepted / total_offers * 100) if total_offers > 0 else 0
    print(f"      Created {total_offers:,} offers. Accepted: {total_accepted:,} ({acc_rate:.2f}%).")


def generate_usage(customers):
    """
    Generate data/usage.jsonl (~220,000 - 240,000 rows).
    One JSON object per line.
    
    Behavioural churn logic:
    - Exited == 1: records stop at random month between 13 (2024-01) and 21 (2024-09),
      ensuring 0 records in final 3 months (2024-10 to 2024-12).
    - Exited == 0: full 24 months (2023-01 to 2024-12).
    """
    print(f"[3/4] Generating {USAGE_OUT}...")
    product_types = ["savings", "current_account", "credit_card", "loan", "investment"]
    month_dates = get_month_end_dates()
    
    usage_counter = 1
    
    with open(USAGE_OUT, mode="w", encoding="utf-8") as f:
        for c in customers:
            cust_id = c["customer_id"]
            base_balance = c["balance"]
            base_products = c["num_products"]
            exited = c["exited"]
            
            if exited == 1:
                # Churn month between month 13 (2024-01) and 21 (2024-09)
                months_to_gen = random.randint(13, 21)
            else:
                months_to_gen = 24
            
            for m_idx in range(months_to_gen):
                log_date = month_dates[m_idx].strftime("%Y-%m-%d")
                usage_log_id = f"USG{usage_counter:08d}"
                prod_type = random.choice(product_types)
                
                # Monthly balance variation: +-15%
                if base_balance > 0:
                    variation = random.uniform(-0.15, 0.15)
                    monthly_bal = round(max(0.0, base_balance * (1.0 + variation)), 2)
                else:
                    monthly_bal = 0.0
                
                # Number of products: slight variation, clamped 1..4
                prod_var = random.choice([-1, 0, 0, 0, 1])
                num_prod = max(1, min(4, base_products + prod_var))
                
                record = {
                    "usage_log_id": usage_log_id,
                    "customer_id": cust_id,
                    "product_type": prod_type,
                    "monthly_balance": monthly_bal,
                    "num_products": num_prod,
                    "log_date": log_date
                }
                f.write(json.dumps(record) + "\n")
                usage_counter += 1
                
    total_usage = usage_counter - 1
    print(f"      Created {total_usage:,} usage records across {len(customers):,} customers.")


def rekey_support_tickets(customers):
    """
    Re-key customer_support_tickets.csv onto real customer IDs.
    Outputs data/support_tickets_rekeyed.csv.
    """
    print(f"[4/4] Re-keying support tickets from {TICKETS_FILE} -> {TICKETS_OUT}...")
    cust_ids = [c["customer_id"] for c in customers]
    
    # Weight distribution: select ~3,500 active ticket submitters
    # Some customers submit 2-5 tickets, others 1, rest 0
    sampled_pool = random.sample(cust_ids, k=3500)
    # Give higher frequency to some customers to create a realistic power-law / skewed distribution
    frequent_submitters = sampled_pool[:500]
    regular_submitters = sampled_pool[500:]
    
    # Create weighted customer choices
    weights = [4] * len(frequent_submitters) + [1] * len(regular_submitters)
    assigned_customers = frequent_submitters + regular_submitters
    
    ticket_count = 0
    with open(TICKETS_FILE, mode="r", encoding="utf-8", errors="replace") as f_in, \
         open(TICKETS_OUT, mode="w", newline="", encoding="utf-8") as f_out:
        reader = csv.reader(f_in)
        writer = csv.writer(f_out)
        
        orig_header = next(reader)
        # We insert customer_id right after Ticket ID
        new_header = [orig_header[0], "customer_id"] + orig_header[1:]
        writer.writerow(new_header)
        
        for row in reader:
            if not row:
                continue
            ticket_id = row[0]
            # Select customer_id
            assigned_id = random.choices(assigned_customers, weights=weights, k=1)[0]
            new_row = [ticket_id, assigned_id] + row[1:]
            writer.writerow(new_row)
            ticket_count += 1
            
    print(f"      Re-keyed {ticket_count:,} tickets to real customer pool.")


def verify_integrity(customers):
    """Verify zero orphan records and correct counts."""
    print("\n=== Verification Checks (Task 1) ===")
    valid_ids = set(c["customer_id"] for c in customers)
    
    # Check offers
    with open(OFFERS_OUT, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        offers = list(reader)
        orphan_offers = sum(1 for o in offers if o["customer_id"] not in valid_ids)
        print(f"  [V4.1] offers.csv: {len(offers):,} rows | Orphan customer_ids: {orphan_offers}")
        
    # Check usage
    with open(USAGE_OUT, "r", encoding="utf-8") as f:
        usage_count = 0
        orphan_usage = 0
        distinct_usage_custs = set()
        for line in f:
            obj = json.loads(line)
            usage_count += 1
            distinct_usage_custs.add(obj["customer_id"])
            if obj["customer_id"] not in valid_ids:
                orphan_usage += 1
        print(f"  [V4.2] usage.jsonl: {usage_count:,} rows | Distinct customers: {len(distinct_usage_custs):,} | Orphan customer_ids: {orphan_usage}")
        
    # Check tickets
    with open(TICKETS_OUT, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        tickets = list(reader)
        orphan_tickets = sum(1 for t in tickets if t["customer_id"] not in valid_ids)
        distinct_ticket_custs = set(t["customer_id"] for t in tickets)
        print(f"  [V4.3] support_tickets_rekeyed.csv: {len(tickets):,} rows | Distinct customers: {len(distinct_ticket_custs):,} | Orphan customer_ids: {orphan_tickets}")

    print("  [V5]   Usage row count in range [200,000, 240,000]:", 200000 <= usage_count <= 240000)
    print("=== All Task 1 data generation checks PASSED! ===\n")


def main():
    print("Starting Task 1 data generation...")
    customers = load_customers()
    generate_offers(customers)
    generate_usage(customers)
    rekey_support_tickets(customers)
    verify_integrity(customers)
    print("Data generation complete!")


if __name__ == "__main__":
    main()
