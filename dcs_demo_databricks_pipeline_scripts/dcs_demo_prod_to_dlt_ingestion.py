
import dlt

jdbc_url = "jdbc:postgresql://dcs-oltp-database.c9mou6iyog9p.ap-southeast-1.rds.amazonaws.com:5432/dcs_test_production_db"
db_user = "dcsdeadmin"
db_pass = "diners12321"
db_schema = "dummy_txn"

tables = [
    "posted_transaction",
    "card_product_info",
    "currency_code",
    "mcc_list",
    "transaction_code",
    "country_code"
]

def create_ingestion_fn(table_name):
    @dlt.table(
        name=f"bronze_{table_name}",
        comment=f"Bronze table: raw {table_name} from Postgres RDS"
    )
    def load_table():
        return (
            spark.read.format("jdbc")
            .option("url", jdbc_url)
            .option("dbtable", f"{db_schema}.{table_name}")
            .option("user", db_user)
            .option("password", db_pass)
            .option("driver", "org.postgresql.Driver")
            .load()
        )
    return load_table

table_functions = [create_ingestion_fn(t) for t in tables]