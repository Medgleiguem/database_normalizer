import pandas as pd
import os
import json
import random
from itertools import product

# --- Configuration ---
benchmark_dir = "./NF_Benchmark"
nf_levels = ["NF1", "NF2", "NF3", "NF4", "NF5"]
tables_per_nf = 3
rows_per_table = 8

os.makedirs(benchmark_dir, exist_ok=True)

# Sample data pools
customers = ["Alice", "Bob", "Charlie", "Diana", "Eve"]
products = ["Apple", "Banana", "Milk", "Bread", "Eggs"]
cities = ["New York", "Los Angeles", "Chicago", "Houston", "Miami"]
phones = [f"{random.randint(100,999)}-{random.randint(100,999)}-{random.randint(1000,9999)}" for _ in range(10)]

# --- Helper functions ---
def multi_value(cell_pool, max_values=2):
    return ", ".join(random.sample(cell_pool, random.randint(1,max_values)))

# --- Generate Benchmark ---
for nf in nf_levels:
    nf_dir = os.path.join(benchmark_dir, f"{nf}_Violations")
    os.makedirs(nf_dir, exist_ok=True)
    
    for t in range(1, tables_per_nf + 1):
        table_name = f"{nf}_Table_{t}"
        data = []

        for r in range(1, rows_per_table + 1):
            order_id = r

            if nf == "NF1":
                # Multi-valued cells
                customer_name = multi_value(customers, 2)
                customer_phones = multi_value(phones, 2)
                city = random.choice(cities)
                product_list = multi_value(products, 3)
                quantity_list = multi_value([str(random.randint(1,10)) for _ in range(5)], 3)
            elif nf == "NF2":
                # Partial dependency: city depends only on customer
                customer_name = random.choice(customers)
                customer_phones = random.choice(phones)
                city = random.choice(cities)
                product_list = random.choice(products)
                quantity_list = random.randint(1,10)
            elif nf == "NF3":
                # Transitive dependency: city -> region
                customer_name = random.choice(customers)
                customer_phones = random.choice(phones)
                city = random.choice(cities)
                region = "East" if city in ["New York", "Miami"] else "West"
                product_list = random.choice(products)
                quantity_list = random.randint(1,10)
            elif nf == "NF4":
                # Multi-valued dependencies: multiple products per order
                customer_name = random.choice(customers)
                customer_phones = random.choice(phones)
                city = random.choice(cities)
                product_list = multi_value(products, 3)
                quantity_list = multi_value([str(random.randint(1,10)) for _ in range(3)], 3)
            elif nf == "NF5":
                # Join dependencies: combine customer, product, supplier
                customer_name = random.choice(customers)
                product = random.choice(products)
                supplier = random.choice(["SupplierA", "SupplierB"])
                order_id = r
                data.append([order_id, customer_name, product, supplier])
                continue  # skip the standard append

            # Append row
            data.append([order_id, customer_name, customer_phones, city, product_list, quantity_list])
        
        # Define columns
        if nf != "NF5":
            columns = ["OrderID", "CustomerName", "CustomerPhones", "City", "Products", "Quantities"]
        else:
            columns = ["OrderID", "CustomerName", "Product", "Supplier"]
        
        df = pd.DataFrame(data, columns=columns)
        excel_path = os.path.join(nf_dir, f"{table_name}_unnormalized.xlsx")
        df.to_excel(excel_path, index=False)

        # Metadata
        metadata = {
            "table_name": table_name,
            "violated_nf": nf,
            "columns": columns,
            "primary_key": ["OrderID"] if nf != "NF5" else ["OrderID", "Product", "Supplier"]
        }
        with open(os.path.join(nf_dir, f"{table_name}_metadata.json"), "w") as f:
            json.dump(metadata, f, indent=4)

print(f"NF1 → NF5 benchmark dataset pack generated at '{benchmark_dir}'")