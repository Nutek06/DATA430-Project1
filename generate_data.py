
import csv
import json
import random
from pathlib import Path
from datetime import datetime, timedelta

import yaml
from faker import Faker

fake = Faker()
random.seed(430)
Faker.seed(430)

DATA_DIR = Path("data")
DATA_DIR.mkdir(parents=True, exist_ok=True)


def save_data(records, name):
    """Save one dataset in CSV, JSON, and YAML formats."""
    csv_path = DATA_DIR / f"{name}.csv"
    json_path = DATA_DIR / f"{name}.json"
    yaml_path = DATA_DIR / f"{name}.yaml"

    with csv_path.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=records[0].keys())
        writer.writeheader()
        writer.writerows(records)

    with json_path.open("w", encoding="utf-8") as file:
        json.dump(records, file, indent=2, default=str)

    with yaml_path.open("w", encoding="utf-8") as file:
        yaml.safe_dump(
            records,
            file,
            sort_keys=False,
            allow_unicode=True
        )

    print(f"Created {csv_path}, {json_path}, and {yaml_path}")


def generate_customers():
    customers = []
    customer_types = ["Regular", "Premium", "VIP"]

    for customer_id in range(1, 1001):
        customers.append({
            "customer_id": customer_id,
            "first_name": fake.first_name(),
            "last_name": fake.last_name(),
            "email": f"customer{customer_id}@example.com",
            "phone": fake.phone_number(),
            "city": fake.city(),
            "registration_date": fake.date_between(
                start_date="-5y", end_date="today"
            ).isoformat(),
            "customer_type": random.choice(customer_types),
        })

    return customers


def generate_products():
    products = []
    categories = [
        "Electronics", "Clothing", "Home", "Books",
        "Beauty", "Sports", "Toys"
    ]
    brands = [
        "Northstar", "BluePeak", "Everline", "Oak & Stone",
        "BrightPath", "SummitWorks"
    ]

    for product_id in range(1, 1001):
        products.append({
            "product_id": product_id,
            "product_name": fake.catch_phrase(),
            "category": random.choice(categories),
            "brand": random.choice(brands),
            "unit_price": round(random.uniform(5, 1500), 2),
            "stock_quantity": random.randint(0, 500),
            "is_active": random.choice([True, False]),
        })

    return products


def generate_orders():
    orders = []
    statuses = ["Pending", "Shipped", "Delivered"]
    payment_methods = [
        "Credit Card", "Debit Card", "PayPal", "Bank Transfer"
    ]

    for order_id in range(1, 1001):
        order_date = fake.date_time_between(
            start_date="-2y", end_date="now"
        )

        orders.append({
            "order_id": order_id,
            "customer_id": random.randint(1, 1000),
            "order_date": order_date.strftime("%Y-%m-%d %H:%M:%S"),
            "order_status": random.choice(statuses),
            "payment_method": random.choice(payment_methods),
            "total_amount": round(random.uniform(20, 2500), 2),
            "discount_amount": round(random.uniform(0, 100), 2),
            "shipping_cost": round(random.uniform(0, 30), 2),
        })

    return orders


def main():
    customers = generate_customers()
    products = generate_products()
    orders = generate_orders()

    # Save each dataset in all three required formats.
    save_data(customers, "customers")
    save_data(products, "products")
    save_data(orders, "orders")

    # Basic validation.
    assert len(customers) == 1000
    assert len(products) == 1000
    assert len(orders) == 1000

    customer_ids = {c["customer_id"] for c in customers}
    assert all(o["customer_id"] in customer_ids for o in orders)

    print("\nSUCCESS: Generated 1,000 records per dataset.")
    print("SUCCESS: All order customer IDs reference valid customers.")
    print(f"Data files are saved in: {DATA_DIR.resolve()}")


if __name__ == "__main__":
    main()
