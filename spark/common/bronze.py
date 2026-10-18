"""Load the source-controlled Bronze DDL and validate existing Iceberg tables."""

from dataclasses import dataclass
from pathlib import Path
import re


DDL_PATH = Path(__file__).resolve().parents[2] / "sql/iceberg/create_bronze_tables.sql"
TABLE_NAMES = ("patients_raw", "encounters_raw", "lab_results_raw", "medications_raw", "vitals_raw", "bed_events_raw")


@dataclass(frozen=True)
class BronzeTable:
    name: str
    ddl: str
    columns: str
    partitions: tuple
    properties: dict


def load_contracts(path=DDL_PATH):
    sql = re.sub(r"--[^\n]*", "", Path(path).read_text(encoding="utf-8"))
    statements = [part.strip() for part in sql.split(";") if part.strip()]
    if not statements or statements[0] != "CREATE NAMESPACE IF NOT EXISTS iceberg.bronze":
        raise ValueError("Bronze DDL must start with the canonical namespace")
    contracts = []
    pattern = (r"CREATE TABLE IF NOT EXISTS iceberg\.bronze\.(\w+)\s*\((.*?)\)\s*USING iceberg"
               r"(?:\s+PARTITIONED BY\s*\((.*?)\))?\s+TBLPROPERTIES\s*\((.*?)\)")
    for statement in statements[1:]:
        match = re.fullmatch(pattern, statement, flags=re.S)
        if not match:
            raise ValueError("Unsupported statement in Bronze DDL")
        name, columns, partition_sql, property_sql = match.groups()
        partitions = []
        if partition_sql:
            for expression in partition_sql.split(","):
                part = re.fullmatch(r"days\((\w+)\)", expression.strip())
                if not part:
                    raise ValueError(f"Unsupported partition expression for {name}")
                partitions.append((part.group(1), "day"))
        properties = dict(re.findall(r"'([^']+)'\s*=\s*'([^']+)'", property_sql))
        contracts.append(BronzeTable(name, statement, columns, tuple(partitions), properties))
    if tuple(table.name for table in contracts) != TABLE_NAMES:
        raise ValueError("Bronze DDL must define exactly the six canonical tables in order")
    return contracts


def validate_table(spark, contract, namespace, warehouse):
    name = f"iceberg.{namespace}.{contract.name}"
    expected = spark.createDataFrame([], contract.columns).schema
    actual = spark.table(name).schema
    signature = lambda schema: [(field.name, field.dataType.simpleString(), field.nullable) for field in schema]
    if signature(actual) != signature(expected):
        raise ValueError(f"{name}: schema drift; apply an explicit migration before bootstrap")
    table = spark._jvm.org.apache.iceberg.spark.Spark3Util.loadIcebergTable(spark._jsparkSession, name)
    partition_fields = table.spec().fields()
    partitions = tuple((table.schema().findColumnName(partition_fields.get(i).sourceId()),
                        partition_fields.get(i).transform().toString()) for i in range(partition_fields.size()))
    if partitions != contract.partitions:
        raise ValueError(f"{name}: partition drift; expected {contract.partitions}, got {partitions}")
    metadata = table.operations().current()
    if str(metadata.formatVersion()) != contract.properties["format-version"]:
        raise ValueError(f"{name}: format version drift")
    for key, value in contract.properties.items():
        if key != "format-version" and table.properties().get(key) != value:
            raise ValueError(f"{name}: table property drift for {key}")
    location = metadata.metadataFileLocation()
    if not location.startswith(warehouse.rstrip("/") + "/"):
        raise ValueError(f"{name}: metadata is outside configured S3 warehouse")
    return location


def bootstrap_bronze(spark, namespace="bronze", warehouse="s3://warehouse/"):
    if not re.fullmatch(r"bronze(?:_smoke_[a-f0-9]{32})?", namespace):
        raise ValueError("Use bronze or an isolated bronze_smoke_<uuid hex> namespace")
    contracts = load_contracts()
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS iceberg.{namespace}")
    # Validate all existing tables before creating missing ones. Never repair
    # schema drift by replacing a table or altering data during normal reruns.
    for contract in contracts:
        if spark.catalog.tableExists(f"iceberg.{namespace}.{contract.name}"):
            validate_table(spark, contract, namespace, warehouse)
    for contract in contracts:
        spark.sql(contract.ddl.replace("iceberg.bronze.", f"iceberg.{namespace}."))
        location = validate_table(spark, contract, namespace, warehouse)
        print(f"PASS: {namespace}.{contract.name} schema/partition/metadata: {location}", flush=True)
    return contracts
