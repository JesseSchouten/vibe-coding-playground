"""
Data producer module for generating sensor event data.

This module provides functionality to generate synthetic sensor event data
in JSON format, which can be used for testing and data pipeline development.
"""

import json
import random
from datetime import datetime
from enum import Enum
from pathlib import Path
from typing import List, Dict, Any, Optional


class SensorEvent(str, Enum):
    """Enumeration of possible sensor events."""
    SENSOR1 = "sensor1"
    SENSOR2 = "sensor2"
    SENSOR3 = "sensor3"


def generate_sensor_data_point() -> Dict[str, Any]:
    """
    Generate a single sensor data point.
    
    Returns:
        Dict containing timestamp, event type, and value.
        Value can be None with probability 1/100.
    """
    # Generate value - 1% chance of None
    value: Optional[int] = None if random.randint(1, 100) == 1 else random.randint(1, 1000)
    
    return {
        "timestamp": datetime.now().isoformat(),
        "event": random.choice(list(SensorEvent)).value,
        "value": value
    }


def generate_sensor_data(count: int) -> List[Dict[str, Any]]:
    """
    Generate multiple sensor data points.
    
    Args:
        count: Number of data points to generate
        
    Returns:
        List of sensor data point dictionaries
        
    Raises:
        ValueError: If count is less than or equal to 0
    """
    if count <= 0:
        raise ValueError("Count must be greater than 0")
    
    return [generate_sensor_data_point() for _ in range(count)]


def write_sensor_data_to_volume(
    data: List[Dict[str, Any]], 
    volume_path: str, 
    filename_prefix: str = "sensor_data"
) -> str:
    """
    Write sensor data to a Databricks volume path as JSON files.
    
    Args:
        data: List of sensor data points to write
        volume_path: Path to the Databricks volume (e.g., '/Volumes/catalog/schema/volume')
        filename_prefix: Prefix for the output filename
        
    Returns:
        Full path to the written file
        
    Raises:
        ValueError: If data is empty
        OSError: If unable to write to the volume path
    """
    if not data:
        raise ValueError("Data cannot be empty")
    
    # Create output directory if it doesn't exist
    output_dir = Path(volume_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    
    # Generate filename with timestamp
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.json"
    output_path = output_dir / filename
    
    # Write data to JSON file
    with open(output_path, 'w') as f:
        json.dump(data, f, indent=2)
    
    return str(output_path)


def generate_and_write_sensor_data(
    count: int,
    volume_path: str,
    filename_prefix: str = "sensor_data"
) -> str:
    """
    Generate sensor data and write it to a Databricks volume in one operation.
    
    Args:
        count: Number of data points to generate
        volume_path: Path to the Databricks volume
        filename_prefix: Prefix for the output filename
        
    Returns:
        Full path to the written file
    """
    data = generate_sensor_data(count)
    return write_sensor_data_to_volume(data, volume_path, filename_prefix)
