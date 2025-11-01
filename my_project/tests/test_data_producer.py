"""
Unit tests for the data producer module.
"""

import json
import pytest
from pathlib import Path
from datetime import datetime
from my_project.data_producer import (
    SensorEvent,
    generate_sensor_data_point,
    generate_sensor_data,
    write_sensor_data_to_volume,
    generate_and_write_sensor_data
)


class TestSensorEvent:
    """Tests for SensorEvent enum."""
    
    def test_sensor_event_values(self):
        """Test that SensorEvent has correct values."""
        assert SensorEvent.SENSOR1.value == "sensor1"
        assert SensorEvent.SENSOR2.value == "sensor2"
        assert SensorEvent.SENSOR3.value == "sensor3"
    
    def test_sensor_event_count(self):
        """Test that there are exactly 3 sensor types."""
        assert len(list(SensorEvent)) == 3


class TestGenerateSensorDataPoint:
    """Tests for generate_sensor_data_point function."""
    
    def test_data_point_structure(self):
        """Test that generated data point has correct structure."""
        data_point = generate_sensor_data_point()
        
        assert "timestamp" in data_point
        assert "event" in data_point
        assert "value" in data_point
    
    def test_timestamp_format(self):
        """Test that timestamp is in ISO format."""
        data_point = generate_sensor_data_point()
        
        # Should be able to parse the timestamp
        datetime.fromisoformat(data_point["timestamp"])
    
    def test_event_is_valid(self):
        """Test that event is one of the valid sensor types."""
        data_point = generate_sensor_data_point()
        
        valid_events = [e.value for e in SensorEvent]
        assert data_point["event"] in valid_events
    
    def test_value_is_valid(self):
        """Test that value is either None or positive integer."""
        # Generate many data points to test value validity
        for _ in range(100):
            data_point = generate_sensor_data_point()
            value = data_point["value"]
            
            assert value is None or (isinstance(value, int) and value > 0)
    
    def test_null_probability(self):
        """Test that null values occur with approximately 1% probability."""
        # Generate many data points to test probability
        null_count = 0
        total_count = 10000
        
        for _ in range(total_count):
            data_point = generate_sensor_data_point()
            if data_point["value"] is None:
                null_count += 1
        
        # Allow for some variance (between 0.5% and 1.5%)
        null_percentage = (null_count / total_count) * 100
        assert 0.5 <= null_percentage <= 1.5


class TestGenerateSensorData:
    """Tests for generate_sensor_data function."""
    
    def test_correct_count(self):
        """Test that correct number of data points is generated."""
        count = 50
        data = generate_sensor_data(count)
        
        assert len(data) == count
    
    def test_each_point_valid(self):
        """Test that each generated point is valid."""
        data = generate_sensor_data(10)
        
        for point in data:
            assert "timestamp" in point
            assert "event" in point
            assert "value" in point
    
    def test_zero_count_raises_error(self):
        """Test that zero count raises ValueError."""
        with pytest.raises(ValueError, match="Count must be greater than 0"):
            generate_sensor_data(0)
    
    def test_negative_count_raises_error(self):
        """Test that negative count raises ValueError."""
        with pytest.raises(ValueError, match="Count must be greater than 0"):
            generate_sensor_data(-5)


class TestWriteSensorDataToVolume:
    """Tests for write_sensor_data_to_volume function."""
    
    def test_write_data_creates_file(self, tmp_path):
        """Test that data is written to file."""
        data = generate_sensor_data(5)
        
        output_path = write_sensor_data_to_volume(
            data, 
            str(tmp_path), 
            "test_sensor_data"
        )
        
        assert Path(output_path).exists()
    
    def test_write_data_correct_format(self, tmp_path):
        """Test that written data is in correct JSON format."""
        data = generate_sensor_data(5)
        
        output_path = write_sensor_data_to_volume(
            data, 
            str(tmp_path), 
            "test_sensor_data"
        )
        
        # Read and verify JSON
        with open(output_path, 'r') as f:
            loaded_data = json.load(f)
        
        assert len(loaded_data) == 5
        assert loaded_data == data
    
    def test_filename_contains_prefix(self, tmp_path):
        """Test that filename contains the specified prefix."""
        data = generate_sensor_data(5)
        prefix = "custom_prefix"
        
        output_path = write_sensor_data_to_volume(
            data, 
            str(tmp_path), 
            prefix
        )
        
        filename = Path(output_path).name
        assert filename.startswith(prefix)
    
    def test_filename_contains_timestamp(self, tmp_path):
        """Test that filename contains timestamp."""
        data = generate_sensor_data(5)
        
        output_path = write_sensor_data_to_volume(
            data, 
            str(tmp_path), 
            "test"
        )
        
        filename = Path(output_path).name
        # Should match pattern: test_YYYYMMDD_HHMMSS.json
        assert filename.endswith(".json")
        assert "_" in filename
    
    def test_empty_data_raises_error(self, tmp_path):
        """Test that empty data raises ValueError."""
        with pytest.raises(ValueError, match="Data cannot be empty"):
            write_sensor_data_to_volume([], str(tmp_path), "test")
    
    def test_creates_directory_if_not_exists(self, tmp_path):
        """Test that function creates directory if it doesn't exist."""
        nested_path = tmp_path / "nested" / "directory"
        data = generate_sensor_data(5)
        
        output_path = write_sensor_data_to_volume(
            data, 
            str(nested_path), 
            "test"
        )
        
        assert Path(output_path).exists()
        assert nested_path.exists()


class TestGenerateAndWriteSensorData:
    """Tests for generate_and_write_sensor_data function."""
    
    def test_generates_and_writes(self, tmp_path):
        """Test that function generates and writes data."""
        count = 10
        
        output_path = generate_and_write_sensor_data(
            count,
            str(tmp_path),
            "integration_test"
        )
        
        # Verify file exists
        assert Path(output_path).exists()
        
        # Verify data
        with open(output_path, 'r') as f:
            data = json.load(f)
        
        assert len(data) == count
        
        # Verify structure of first data point
        assert "timestamp" in data[0]
        assert "event" in data[0]
        assert "value" in data[0]
