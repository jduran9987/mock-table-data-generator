"""
Metadata Management Module for Synthetic Data Generation

This module provides functionality for tracking and managing unique
identifiers across multiple data generation runs. It ensures referential
integrity by maintaining persistent state of generated IDs for database
tables, enabling proper foreign key relationships in synthetic relational
data.

The MetadataManager class handles:
- Loading and saving metadata state to JSON files
- Generating sequential ID ranges for new records
- Tracking existing IDs for foreign key references
- Supporting multiple table types (users, orders, products)

Typical usage:
    manager = MetadataManager("data_metadata.json")
    id_range = manager.get_next_id_range("users", 100)
    # Generate data using the ID range
    manager.update_last_id("users", max(id_range))
    manager.save_metadata()
"""
import json
import os
from typing import Any


class MetadataManager:
    """Manages metadata file for tracking IDs across generation runs.
    
    This class provides persistent storage and management of unique
    identifiers for synthetic data generation, ensuring referential
    integrity across multiple generation runs without requiring a database.
    """

    def __init__(self, metadata_file: str = "metadata.json") -> None:
        """Initialize the MetadataManager with a specified metadata file.
        
        Args:
            metadata_file: Path to the JSON file for storing metadata.
                          Defaults to "metadata.json".
        """
        self.metadata_file = metadata_file
        self.metadata = self._load_metadata()
    
    def _load_metadata(self) -> dict[str, dict[str, Any]]:
        """Load existing metadata from file or create initial structure.
        
        Returns:
            Dictionary containing metadata for each table with structure:
            {
                "table_name": {
                    "last_id": int,
                    "existing_ids": list[int]
                }
            }
            
        Note:
            If metadata file doesn't exist, returns default structure for
            users, orders, and products tables with last_id=0 and empty
            existing_ids lists.
        """
        if os.path.exists(self.metadata_file):
            with open(self.metadata_file) as f:
                return json.load(f)
        return {
            "users": {"last_id": 0, "existing_ids": []},
            "orders": {"last_id": 0, "existing_ids": []},
            "products": {"last_id": 0, "existing_ids": []}
        }
    
    def save_metadata(self) -> None:
        """Save current metadata state to the JSON file.
        
        Persists all tracked IDs and last_id values to disk for use in
        subsequent data generation runs.
        
        Raises:
            IOError: If unable to write to the metadata file.
        """
        with open(self.metadata_file, "w") as f:
            json.dump(self.metadata, f, indent=2)
    
    def get_next_id_range(
        self,
        table_name: str,
        count: int
    ) -> range:
        """Get range of sequential IDs for new records.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
            count: Number of sequential IDs needed.
            
        Returns:
            Range object containing sequential integers starting from
            (last_id + 1) and containing 'count' numbers.
            
        Example:
            If last_id for "users" is 100 and count is 5,
            returns range(101, 106) which gives 
            [101, 102, 103, 104, 105].
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        start_id = self.metadata[table_name]["last_id"] + 1
        end_id = start_id + count
        return range(start_id, end_id)
    
    def update_last_id(
        self,
        table_name: str,
        last_id: int
    ) -> None:
        """Update the last generated ID for a specific table.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
            last_id: The highest ID that was generated in the current run.
            
        Note:
            This should be called after generating data to ensure the next
            generation run starts with the correct ID sequence.
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        self.metadata[table_name]["last_id"] = last_id
    
    def get_existing_ids(
        self,
        table_name: str
    ) -> list[int]:
        """Get list of all existing IDs for a table.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
            
        Returns:
            List of integers representing all IDs that have been generated
            for the specified table across all previous runs.
            
        Note:
            This is primarily used for foreign key references when 
            generating related data (e.g., orders referencing existing 
            user IDs).
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        return self.metadata[table_name]["existing_ids"]

    def add_generated_ids(
        self, 
        table_name: str,
        new_ids: list[int]
    ) -> None:
        """Add newly generated IDs to the existing list for a table.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
            new_ids: List of integers representing IDs that were generated
                    in the current run.
                    
        Note:
            This extends the existing_ids list, maintaining a complete
            history of all generated IDs for foreign key reference 
            purposes. Should be called after generating data but before 
            saving metadata.
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        self.metadata[table_name]["existing_ids"].extend(new_ids)
