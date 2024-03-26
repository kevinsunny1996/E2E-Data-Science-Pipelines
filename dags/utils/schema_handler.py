from dataclasses import dataclass
from typing import List

@dataclass
class SchemaField:
    name: str
    type: str
    mode: str
    description: str

@dataclass
class CSVFileSchema:
    product_table_name: str
    schema_fields: List[SchemaField]