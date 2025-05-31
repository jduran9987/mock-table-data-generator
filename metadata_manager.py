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
- Inferring existing IDs from last_id for foreign key references
- Supporting multiple table types (users, orders, products)

Optimization: Since IDs are generated sequentially starting from 1,
we only store the last_id and infer all existing IDs as range(1, last_id+1).
This dramatically reduces metadata file size and improves performance.

Typical usage:
    manager = MetadataManager("data_metadata.json")
    id_range = manager.get_next_id_range("users", 100)
    # Generate data using the ID range
    manager.update_last_id("users", max(id_range))
    manager.save_metadata()
"""
import json
import os
import random
from typing import Any


class MetadataManager:
    """Manages metadata file for tracking IDs across generation runs.
    
    This class provides persistent storage and management of unique
    identifiers for synthetic data generation, ensuring referential
    integrity across multiple generation runs without requiring a database.
    
    Optimized to store only last_id values, inferring existing IDs from
    sequential generation pattern.
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
                    "last_id": int
                }
            }
            
        Note:
            If metadata file doesn't exist, returns default structure for
            users, orders, and products tables with last_id=0.
            Existing IDs are inferred as range(1, last_id+1).
        """
        if os.path.exists(self.metadata_file):
            metadata = json.load(open(self.metadata_file))
            # Handle legacy format with existing_ids lists
            for table_name, table_data in metadata.items():
                if "existing_ids" in table_data:
                    # Convert legacy format: use max of existing_ids as last_id
                    if table_data["existing_ids"]:
                        table_data["last_id"] = max(table_data["existing_ids"])
                    # Remove the unnecessary existing_ids list
                    del table_data["existing_ids"]
            return metadata
        
        return {
            "users": {"last_id": 0},
            "orders": {"last_id": 0},
            "products": {"last_id": 0}
        }
    
    def save_metadata(self) -> None:
        """Save current metadata state to the JSON file.
        
        Persists only last_id values to disk for use in subsequent data
        generation runs. Existing IDs are inferred from last_id.
        
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
            Generated as range(1, last_id + 1) converted to list.
            
        Note:
            This is primarily used for foreign key references when 
            generating related data (e.g., orders referencing existing 
            user IDs). IDs are inferred from last_id since generation
            is sequential starting from 1.
            
            WARNING: For large datasets (10K+ records), this creates
            large lists in memory. Consider using get_random_existing_id()
            or get_existing_id_count() for better performance.
            
        Example:
            If last_id is 1000, returns [1, 2, 3, ..., 1000].
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        last_id = self.metadata[table_name]["last_id"]
        if last_id == 0:
            return []
        return list(range(1, last_id + 1))
    
    def get_random_existing_id(self, table_name: str) -> int:
        """Get a random existing ID for foreign key relationships.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
                       
        Returns:
            Random integer ID from the range of existing IDs.
            
        Note:
            More memory-efficient than get_existing_ids() for large
            datasets since it doesn't create the full list.
            
        Raises:
            KeyError: If table_name is not found in metadata.
            ValueError: If no existing IDs are available (last_id = 0).
        """
        last_id = self.metadata[table_name]["last_id"]
        if last_id == 0:
            raise ValueError(f"No existing IDs for table {table_name}")
        
        return random.randint(1, last_id)
    
    def get_existing_id_count(self, table_name: str) -> int:
        """Get count of existing IDs for a table.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
                       
        Returns:
            Number of existing IDs (equal to last_id).
            
        Note:
            Useful for checking if table has data before generating
            dependent records.
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        return self.metadata[table_name]["last_id"]

    def add_generated_ids(
        self, 
        table_name: str,
        new_ids: list[int]
    ) -> None:
        """Update last_id based on newly generated IDs.
        
        Args:
            table_name: Name of the table ("users", "orders", or 
                       "products").
            new_ids: List of integers representing IDs that were generated
                    in the current run.
                    
        Note:
            Since IDs are generated sequentially, this method simply
            updates last_id to the maximum of the new_ids. The complete
            existing ID list is inferred from the updated last_id.
            
        Raises:
            KeyError: If table_name is not found in metadata.
        """
        if new_ids:
            new_last_id = max(new_ids)
            # Ensure we're not going backwards (should not happen with 
            # sequential generation)
            current_last_id = self.metadata[table_name]["last_id"]
            self.metadata[table_name]["last_id"] = max(
                current_last_id, new_last_id
            )
