"""
S3 Upload Management Module for Synthetic Data

This module provides intelligent file upload capabilities for synthetic data
distribution to Amazon S3. It handles automatic file size management,
chunking large datasets, and maintaining organized storage structures.

Key features:
- Automatic file size estimation and chunking
- Timestamped file naming for data versioning
- Intelligent row-density calculations for accurate chunking
- Temporary file management with proper cleanup
- Organized S3 key structures for easy data discovery

The S3Uploader class manages:
- Single file uploads for datasets under size limits
- Multi-part chunking for large datasets exceeding limits
- Sample-based size estimation for accurate chunk sizing
- UTC timestamp generation for consistent file naming
- Error handling and cleanup for temporary files

Typical usage:
    uploader = S3Uploader("my-data-bucket")
    
    # Single file upload (no size limit)
    keys = uploader.upload_dataframe(small_df, "users")
    
    # Chunked upload (50 MB limit per file)
    keys = uploader.upload_dataframe(large_df, "orders", 50)
    
    # Results in S3 keys like:
    # users/202505301430.parquet
    # orders/202505301430_part_001.parquet
    # orders/202505301430_part_002.parquet
"""

import os
from datetime import datetime, timezone

import boto3
import pandas as pd


class S3Uploader:
    """Handles S3 uploads with intelligent file size management.
    
    Provides automated chunking and upload capabilities for pandas
    DataFrames to S3, with size-based splitting and organized naming
    conventions for efficient data storage and retrieval.
    """

    def __init__(self, bucket_name: str) -> None:
        """Initialize S3 uploader with target bucket configuration.
        
        Args:
            bucket_name: Name of the S3 bucket for data uploads.
                        Bucket must exist and be accessible with current
                        AWS credentials.
        """
        self.bucket_name = bucket_name
        self.s3_client = boto3.client("s3")
    
    def upload_dataframe(
        self,
        df: pd.DataFrame,
        table_name: str,
        file_size_limit_in_mb: int | None = None
    ) -> list[str]:
        """Upload DataFrame to S3 with optional file size chunking.
        
        Args:
            df: DataFrame containing data to upload.
            table_name: Base name for S3 key organization 
                       (e.g., "users", "orders").
            file_size_limit_in_mb: Maximum file size in MB. If None,
                                  uploads as single file regardless of size.
                                  If specified, splits data into chunks.
                                  
        Returns:
            List of S3 keys where files were uploaded. Single key for
            non-chunked uploads, multiple keys for chunked uploads.
            
        Example:
            # Single file: ["users/202505301430.parquet"]
            # Chunked: ["orders/202505301430_part_001.parquet",
            #          "orders/202505301430_part_002.parquet"]
            
        Note:
            Uses UTC timestamps for consistent file naming across
            time zones. Temporary files are automatically cleaned up.
        """
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
        uploaded_keys = []

        if file_size_limit_in_mb is None:
            # Upload as a single file
            key = f"{table_name}/{timestamp}.parquet"
            self._upload_single_file(df, key)
            uploaded_keys.append(key)
        else:
            # Split into chunks based on file size
            chunks = self._split_dataframe_by_size(df, file_size_limit_in_mb)
            for i, chunk in enumerate(chunks):
                key = f"{table_name}/{timestamp}_part_{i+1:03d}.parquet"
                self._upload_single_file(chunk, key)
                uploaded_keys.append(key)
        
        return uploaded_keys
    
    def _upload_single_file(
        self,
        df: pd.DataFrame,
        key: str
    ) -> None:
        """Upload single DataFrame as parquet file to S3.
        
        Args:
            df: DataFrame to upload.
            key: S3 key (path) for the uploaded file.
            
        Note:
            Creates temporary parquet file locally, uploads to S3,
            then removes temporary file. Handles cleanup even if
            upload fails.
            
        Raises:
            boto3 exceptions: If S3 upload fails due to permissions,
                             network issues, or invalid bucket.
        """
        # Create temporary file 
        temp_file = f"/tmp/{key.replace('/', '_')}"
        df.to_parquet(temp_file, index=False)

        try:
            self.s3_client.upload_file(temp_file, self.bucket_name, key)
            print(f"Uploaded: s3://{self.bucket_name}/{key}")
        finally:
            # Clean up temp file
            if os.path.exists(temp_file):
                os.remove(temp_file)
    
    def _split_dataframe_by_size(
        self,
        df: pd.DataFrame,
        size_limit_mb: int
    ) -> list[pd.DataFrame]:
        """Split DataFrame into chunks based on target file size.
        
        Args:
            df: DataFrame to split into size-based chunks.
            size_limit_mb: Target maximum size per chunk in megabytes.
            
        Returns:
            List of DataFrame chunks, each estimated to be under
            the size limit when saved as parquet.
            
        Note:
            Uses sampling approach to estimate compression and data
            density. Takes up to 1000 rows as sample, creates temporary
            parquet file to measure actual compressed size, then
            calculates rows per MB for accurate chunk sizing.
            
        Example:
            If sample shows 400 rows = 1 MB, and limit is 10 MB,
            each chunk will contain ~4000 rows.
        """
        # Estimate rows per MB using sample data
        sample_size = min(1000, len(df))
        sample_df = df.head(sample_size)

        # Create temp file to estimate compressed size
        temp_file = "/tmp/sample.parquet"
        sample_df.to_parquet(temp_file, index=False)

        try:
            file_size_mb = os.path.getsize(temp_file) / (1024 * 1024)
            rows_per_mb = (
                sample_size / file_size_mb if file_size_mb > 0 else 1000
            )
            chunk_size = int(rows_per_mb * size_limit_mb)
        finally:
            if os.path.exists(temp_file):
                os.remove(temp_file)
        
        # Split DataFrame into chunks
        chunks = []
        for i in range(0, len(df), chunk_size):
            chunks.append(df.iloc[i:i + chunk_size])
        
        return chunks
