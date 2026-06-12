import os
from azure.kusto.data import KustoClient, KustoConnectionStringBuilder

EVENTHOUSE_CLUSTER_URI = "https://trd-9xcasgtnw87tra1q6w.z6.kusto.fabric.microsoft.com"
EVENTHOUSE_DATABASE = "SentinelMesh_Eventhouse"

kcsb = KustoConnectionStringBuilder.with_aad_device_authentication(EVENTHOUSE_CLUSTER_URI)
client = KustoClient(kcsb)

for table in ["fact_transactions", "fact_alerts"]:
    print(f"\nSchema for: {table}")
    try:
        res = client.execute(EVENTHOUSE_DATABASE, f".show table {table} schema as json")
        import json
        schema_json = json.loads(res.primary_results[0].rows[0][1])
        columns = [col["Name"] for col in schema_json["OrderedColumns"]]
        print(f"Columns: {', '.join(columns)}")
    except Exception as e:
        print(f"Error: {e}")
