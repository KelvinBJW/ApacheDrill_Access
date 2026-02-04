# DrillClient Usage Guide

A lightweight Python wrapper for the **Apache Drill REST API**. Bypasses ODBC/JDBC driver complexities and handles secure MapR clusters with safe date parsing.

## 🚀 Quick Start Example

This example shows how to connect, find a hidden Oracle synonym (or table), and query it while letting the client handle the `9999-12-31` dates automatically.

```python
from drill_client import DrillClient

# 1. Initialize the client (Auto-authenticates)
client = DrillClient(
    hostname="drill-dev-server.company.com", 
    username="your_user", 
    password="your_password"
)

# 2. Browse available storage plugins
all_schemas = client.list_schemas()
print(f"Available Schemas: {all_schemas}")

# 3. Discover objects in a specific Oracle schema
# This handles the Oracle Data Dictionary (Synonyms, Links, Tables)
objects = client.list_objects("oracle.FINANCE_PROD")
for obj in objects:
    print(f"Found {obj['TYPE']}: {obj['NAME']}")

# 4. Execute a query with "High Dates" (e.g., 9999-12-31)
# The client automatically detects DATE/TIMESTAMP metadata and 
# converts Unix MS to Python datetime objects.
sql = "SELECT EMP_ID, HIRE_DATE, TERMINATION_DATE FROM `oracle`.`FINANCE_PROD`.`EMPLOYEES` LIMIT 100"
df = client.execute(sql)

# 5. Display the data
# Termination dates like 9999-12-31 will be valid datetime objects, not NaT.
print(df[['EMP_ID', 'TERMINATION_DATE']].head())

# 6. Perform Python-level filtering on dates
active_emp = df[df['TERMINATION_DATE'].apply(lambda x: x.year == 9999)]
print(f"Number of active employees: {len(active_emp)}")
