import requests
import urllib3
import pandas as pd
import logging
import time
from datetime import datetime, timedelta

# Suppress SSL warnings
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Configure Logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("drill_client.log"),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("DrillClient")

class DrillClient:
    def __init__(self, hostname, username, password):
        self.base_url = f"https://{hostname}:8047"
        self.session = requests.Session()
        self.hostname = hostname
        self._authenticate(username, password)

    def _authenticate(self, username, password):
        login_url = f"{self.base_url}/j_security_check"
        payload = {"j_username": username, "j_password": password}
        try:
            logger.info(f"Attempting connection to {self.hostname}...")
            response = self.session.post(login_url, data=payload, verify=False, timeout=10)
            response.raise_for_status()
            logger.info("Authentication successful.")
        except Exception as e:
            logger.error(f"Authentication failed for {username}: {e}")
            self.session = None

    @staticmethod
    def _ms_to_datetime_safe(ms):
        if pd.isna(ms) or ms == "" or ms is None:
            return None
        try:
            return datetime(1970, 1, 1) + timedelta(milliseconds=float(ms))
        except (ValueError, OverflowError):
            logger.warning(f"Timestamp out of range: {ms}")
            return None

    def execute(self, sql_query):
        if not self.session:
            logger.error("No active session. Query aborted.")
            return None
            
        start_time = time.time()
        query_url = f"{self.base_url}/query.json"
        payload = {"queryType": "SQL", "query": sql_query}
        
        try:
            response = self.session.post(query_url, json=payload, verify=False, timeout=120)
            duration = round(time.time() - start_time, 2)
            
            if response.status_code != 200:
                logger.error(f"Query Failed ({duration}s): {response.text}")
                return None
            
            data = response.json()
            rows = data.get('rows', [])
            df = pd.DataFrame(rows)
            
            logger.info(f"Query completed in {duration}s. Returned {len(rows)} rows.")

            # Metadata-based date conversion
            col_type_map = dict(zip(data.get('columns', []), data.get('metadata', [])))
            for col in df.columns:
                if col_type_map.get(col, "").upper() in ["DATE", "TIME", "TIMESTAMP"]:
                    df[col] = df[col].apply(self._ms_to_datetime_safe)
            return df
        except Exception as e:
            logger.exception(f"Request Exception during query: {e}")
            return None

    def list_schemas(self):
        logger.info("Fetching storage schemas...")
        sql = "SELECT SCHEMA_NAME FROM INFORMATION_SCHEMA.SCHEMATA ORDER BY SCHEMA_NAME"
        df = self.execute(sql)
        return df['SCHEMA_NAME'].tolist() if df is not None else []

    def list_objects(self, schema_name):
        logger.info(f"Listing objects in schema: {schema_name}")
        # Filesystem
        parts = schema_name.split('.')
        plugin_root = parts[0]
        print(plugin_root)
        if plugin_root.lower() in ["dfs", "cp"]:
            df = self.execute(f"SHOW FILES IN `{schema_name}`")
            return [{"NAME": r['name'], "TYPE": "FILE/VIEW"} for _, r in df.iterrows()] if df is not None else []

        # Oracle/RDBMS
        df = self.execute(f"SHOW TABLES IN `{schema_name}`")
        df.rename(columns={"TABLE_NAME": "NAME"}, inplace=True)
        if df is None or df.empty:
            parts = schema_name.split('.')
            plugin_root = parts[0]
            owner_filter = f"= '{parts[1].upper()}'" if len(parts) > 1 else "IS NOT NULL"
            sql = f"SELECT OWNER || '.' || OBJECT_NAME AS NAME, OBJECT_TYPE AS TYPE FROM `{plugin_root}`.SYS.ALL_OBJECTS WHERE OBJECT_TYPE IN ('TABLE', 'VIEW', 'SYNONYM') AND OWNER {owner_filter} AND OWNER NOT IN ('SYS', 'SYSTEM') ORDER BY OWNER, OBJECT_NAME"
            df = self.execute(sql)
        return df.to_dict(orient='records') if df is not None else []
