import logging
import os

from mcp.server.fastmcp import FastMCP
from pyiceberg.catalog import Catalog
from pyiceberg.catalog.rest import RestCatalog
from pyiceberg.partitioning import PartitionSpec, UNPARTITIONED_PARTITION_SPEC
from pyiceberg.schema import Schema
from pyiceberg.table.sorting import UNSORTED_SORT_ORDER, SortOrder
from pyiceberg.types import (
    TimestampType,
    FloatType,
    DoubleType,
    StringType,
    NestedField,
    IntegerType,
    LongType,
    BooleanType,
    DateType,
    TimeType,
    TimestamptzType,
    UUIDType,
    BinaryType,
)



catalog_uri = "http://localhost:8181"
s3_endpoint = "http://minio:9000"
minio_user = os.environ.get("ICEBERG_MINIO_USER")
minio_password = os.environ.get("ICEBERG_MINIO_PASSWORD")

logger = logging.getLogger("iceberg-mcp")

mcp = FastMCP("iceberg-mcp", "Iceberg MCP Server", version="0.1.0")

@mcp.tool()
def get_namespaces() -> str:
    """Provides a list of namespaces from the Iceberg catalog."""
    catalog = get_catalog()
    namespaces = catalog.list_namespaces()
    return "\n".join(ns[0] for ns in namespaces)


@mcp.tool()
def get_iceberg_tables(namespace: str) -> str:
    """Provides a list of iceberg tables from the Iceberg catalog for a given namespace"""
    catalog = get_catalog()
    tables = catalog.list_tables(namespace)
    return "\n".join(t[1] for t in tables)

@mcp.tool()
def create_namespace(
        namespace: str
) -> str:
    """Creates a new namespace in the Iceberg catalog."""
    catalog = get_catalog()
    catalog.create_namespace(namespace)
    return f"Namespace '{namespace}' created successfully."


@mcp.tool()
def create_table(
        namespace: str,
        table_name: str,
        schema: str,
        partition_spec: str
) -> str:
    """Creates a new Iceberg table in the specified namespace."""
    catalog = get_catalog()
    fields = []
    columns_str = schema.split(',')
    for i, col_def in enumerate(columns_str):
        parts = col_def.strip().split()
        if len(parts) == 3 and parts[1].lower() == 'type':
            name = parts[0]
            type_str = parts[2]
            fields.append(NestedField(field_id=i + 1, name=name, field_type=string_to_type(type_str), required=True))
        else:
            raise ValueError(f"Invalid column definition: '{col_def.strip()}'. Expected 'name type type'.")
                                                                                                                                                                                                                            
    iceberg_schema = Schema(*fields)
                                                                                                                                                                                                                            
    iceberg_partition_spec = UNPARTITIONED_PARTITION_SPEC
    if partition_spec:
        from pyiceberg.partitioning import PartitionField
        partition_fields = []
        for col_name in partition_spec.split(','):
            col_name = col_name.strip()
            try:
                source_field = iceberg_schema.find_field(col_name)
                partition_fields.append(PartitionField(source_id=source_field.field_id, name=col_name))
            except ValueError:
                raise ValueError(f"Partition column '{col_name}' not found in schema.")
        iceberg_partition_spec = PartitionSpec(*partition_fields, spec_id=0)
                                                                                                                                                                                                                            
  
    table_obj = catalog.create_table(
        (namespace, table_name),
        schema=iceberg_schema,
        partition_spec=iceberg_partition_spec
    )
    return f"Table '{table_obj.name}' created successfully in namespace '{namespace}'."

@mcp.tool()
def get_table_properties(
        namespace: str,
        table_name: str
) -> dict:
    catalog: Catalog = get_catalog()
    table_obj = catalog.load_table((namespace, table_name))
    partition_specs = [p.model_dump() for p in table_obj.metadata.partition_specs]
    sort_orders = [s.model_dump() for s in table_obj.metadata.sort_orders]
    current_snapshot = table_obj.current_snapshot()
    if not current_snapshot or not current_snapshot.summary:
        return {}
    return {
        "total_size_in_bytes": current_snapshot.summary["total-files-size"],
        "total_records": current_snapshot.summary["total-records"],
        "partition_specs": partition_specs,
        "sort_orders": sort_orders,
        **table_obj.properties
    }


@mcp.tool()
def get_table_partitions(
        namespace: str,
        table_name: str
) -> list[dict[str, int]]:
    """Provides the partitions for a given Iceberg table""" 
    catalog: Catalog = get_catalog()
    table_obj = catalog.load_table((namespace, table_name))
    partitions = table_obj.inspect.partitions().to_pylist()

    result = []
    for p in partitions:
        result.append(
            {
                "partition": p['partition'],
                "record_count": p['record_count'],
                "size_in_bytes": p['total_data_file_size_in_bytes'],
            }
        )
    return result


def get_catalog() -> RestCatalog:
    try:
        catalog = RestCatalog(
            "rest-catalog",
            **{
                "uri": catalog_uri,
                "s3.endpoint": s3_endpoint,
                "s3.access-key-id": minio_user,
                "s3.secret-access-key": minio_password,
            }
        )
    except Exception as e:
        logger.error(f"Error creating AWS connection: {str(e)}")
        raise
    return catalog

def string_to_type(type_str: str):
    type_str = type_str.lower()
    if type_str == 'string':
        return StringType()
    elif type_str == 'int' or type_str == 'integer':
        return IntegerType()
    elif type_str == 'long':
        return LongType()
    elif type_str == 'float':
        return FloatType()
    elif type_str == 'double':
        return DoubleType()
    elif type_str == 'boolean':
        return BooleanType()
    elif type_str == 'date':
        return DateType()
    elif type_str == 'time':
        return TimeType()
    elif type_str == 'timestamp':
        return TimestampType()
    elif type_str == 'timestamptz':
        return TimestamptzType()
    elif type_str == 'uuid':
        return UUIDType()
    elif type_str == 'binary':
        return BinaryType()
    else:
        raise ValueError(f"Unsupported type: {type_str}")
                                                                                                                                                                                                                                


def main() -> None:
    logger.info("Starting Iceberg MCP Server")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
