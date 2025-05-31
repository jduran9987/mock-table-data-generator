"""
Synthetic Data Generation Application

This module provides a command-line application for generating synthetic
relational database data and uploading it to Amazon S3. It orchestrates
the entire data generation pipeline from metadata management through
S3 distribution.

The application handles:
- Command-line argument parsing and validation
- Coordinated data generation across multiple table types
- Referential integrity maintenance between related tables
- Intelligent file chunking and S3 upload management
- Metadata persistence for incremental data generation
- Error handling and progress reporting

Key capabilities:
- Generates realistic user demographics, product catalogs, and orders
- Maintains foreign key relationships across generation runs
- Supports incremental data addition to existing datasets
- Provides flexible file size management for large datasets
- Creates timestamped, organized data structures in S3

Typical usage:
    # Generate initial dataset
    python synthetic_data_app.py --table-names users,products,orders \\
        --num-rows 1000 --s3-bucket my-data-bucket
    
    # Add more data with size limits
    python synthetic_data_app.py --table-names orders \\
        --num-rows 5000 --file-size-limit 50 --s3-bucket my-data-bucket
    
    # Results in S3 structure:
    # users/202505301430.parquet
    # products/202505301430.parquet  
    # orders/202505301430_part_001.parquet
    # orders/202505301430_part_002.parquet

Dependencies:
    - data_generators: Table-specific synthetic data generation
    - metadata_manager: ID tracking and referential integrity
    - s3_uploader: Intelligent file upload and chunking
"""

import argparse

from data_generators import (
    ProductDataGenerator, 
    OrderDataGenerator, 
    UserDataGenerator
)
from metadata_manager import MetadataManager
from s3_uploader import S3Uploader


class SyntheticDataApplications:
    """Main application orchestrating synthetic data generation pipeline.
    
    Coordinates data generation across multiple table types while
    maintaining referential integrity and providing organized S3 upload
    capabilities. Handles the complete workflow from metadata loading
    through final data distribution.
    """

    def __init__(self) -> None:
        """Initialize application with data generators and metadata manager.
        
        Sets up metadata management for ID tracking and creates generators
        for each supported table type. All generators share the same
        metadata manager to ensure referential integrity.
        """
        self.metadata_manager = MetadataManager()
        self.generators = {
            "users": UserDataGenerator(self.metadata_manager),
            "orders": OrderDataGenerator(self.metadata_manager),
            "products": ProductDataGenerator(self.metadata_manager)
        }
    
    def run(
        self,
        table_names: list[str],
        num_rows: int,
        s3_bucket: str,
        file_size_limit_mb: int | None = None
    ) -> None:
        """Execute complete data generation and upload pipeline.
        
        Args:
            table_names: List of table names to generate data for.
                        Supported: "users", "orders", "products".
            num_rows: Number of records to generate per table.
            s3_bucket: Target S3 bucket for data uploads.
            file_size_limit_mb: Optional size limit in MB per file.
                               If None, uploads single files regardless
                               of size. If specified, chunks large files.
                               
        Note:
            Tables with foreign key dependencies (orders) require
            referenced tables (users, products) to exist in metadata.
            Generate base tables first, then dependent tables.
            
        Example:
            # Safe order: users -> products -> orders
            app.run(["users", "products"], 1000, "bucket")
            app.run(["orders"], 2000, "bucket")
        """

        print(f"Starting data generation for tables: "
              f"{', '.join(table_names)}")
        print(f"Generating {num_rows} rows per table")

        uploader = S3Uploader(s3_bucket)

        for table_name in table_names:
            if table_name not in self.generators:
                print(f"Warning: Unknown table '{table_name}', skipping...")
                continue
                
            print(f"\nGenerating data for {table_name}...")

            try:
                # Generate data using appropriate generator
                generator = self.generators[table_name]
                df = generator.generate_data(table_name, num_rows)

                print(f"Generated {len(df)} rows for {table_name}")

                # Upload to S3 with optional chunking
                uploaded_keys = uploader.upload_dataframe(
                    df, table_name, file_size_limit_mb
                )

                print(f"Uploaded {len(uploaded_keys)} file(s) for "
                      f"{table_name}")
                      
            except Exception as e:
                print(f"Error generating data for {table_name}: {str(e)}")
                continue
        
        # Persist metadata for future runs
        self.metadata_manager.save_metadata()
        print(f"\nMetadata saved to "
              f"{self.metadata_manager.metadata_file}")
        print("Data generation complete!")


def main():
    """Parse command-line arguments and execute data generation.
    
    Provides command-line interface for the synthetic data generation
    application. Handles argument parsing, validation, and application
    execution with proper error handling.
    
    Command-line arguments:
        --table-names: Comma-separated list of tables to generate
        --num-rows: Number of records per table
        --s3-bucket: Target S3 bucket name
        --file-size-limit: Optional max file size in MB
        
    Example:
        python app.py --table-names users,orders \\
            --num-rows 1000 --s3-bucket my-bucket \\
            --file-size-limit 25
    """
    parser = argparse.ArgumentParser(
        description="Generate synthetic relational database data"
    )
    parser.add_argument(
        "--table-names",
        required=True,
        help="Comma-separated table names (e.g., users,orders,products)"
    )
    parser.add_argument(
        "--num-rows",
        type=int,
        required=True,
        help="Number of rows to generate for each table"
    )
    parser.add_argument(
        "--file-size-limit",
        type=int,
        help="Maximum file size in MB (optional)"
    )
    parser.add_argument(
        "--s3-bucket",
        required=True,
        help="S3 bucket name for uploads"
    )

    args = parser.parse_args()

    # Parse and clean table names
    table_names = [name.strip() for name in args.table_names.split(",")]

    # Create and execute application
    app = SyntheticDataApplications()
    app.run(
        table_names=table_names,
        num_rows=args.num_rows,
        s3_bucket=args.s3_bucket,
        file_size_limit_mb=args.file_size_limit
    )


if __name__ == "__main__":
    main()
