from dataclasses import dataclass
from enum import Enum
from typing import Optional
from pathlib import Path

class OutputMode(str, Enum):
    CSV = 'csv'
    KAFKA = 'kafka'

@dataclass
class GeneratorConfig:
    output_mode: OutputMode
    csv_export_folder: Path = Path.cwd()
    kafka_bootstrap_servers: Optional[str] = None
    kafka_topic: Optional[str] = None
