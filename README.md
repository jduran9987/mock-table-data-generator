# Synthetic Data Generator

A comprehensive Python application for generating realistic synthetic relational database data with proper foreign key relationships, intelligent file management, and S3 distribution capabilities.

## Overview

This project creates synthetic data that mimics real-world e-commerce databases, maintaining referential integrity across multiple tables while providing flexible data generation and distribution options. Perfect for testing, development, analytics, and machine learning applications that require realistic datasets.

## Features

### 🎯 Realistic Data Generation
- **Users**: Comprehensive demographics, account status, preferences, and behavioral data
- **Products**: Complete product catalogs with inventory, pricing, ratings, and supplier information  
- **Orders**: Full transaction lifecycle with payments, shipping, and customer service data
- **Relationships**: Proper foreign key constraints with realistic data distributions

### 🔄 Incremental Generation
- Persistent metadata tracking across runs
- Seamless addition of new data to existing datasets
- Automatic ID sequencing to prevent conflicts
- Foreign key relationship maintenance

### 📦 Intelligent File Management
- Automatic file size estimation and chunking
- Configurable file size limits for optimal storage
- Timestamped file naming for data versioning
- Parquet format for efficient storage and querying

### ☁️ S3 Integration
- Direct upload to Amazon S3 buckets
- Organized folder structure by table type
- Multi-part upload for large datasets
- Automatic cleanup of temporary files

## Installation

### Prerequisites
- Python 3.9+
- AWS credentials configured (for S3 uploads)
- Required Python packages (see requirements.txt)

### Setup
```bash
# Clone the repository
git clone <repository-url>
cd synthetic-data-generator

# Install dependencies
pip install -r requirements.txt

# Configure AWS credentials (if not already done)
aws configure
```

### Requirements
```
pandas>=1.5.0
boto3>=1.26.0
faker>=18.0.0
numpy>=1.24.0
pyarrow>=11.0.0
```

## Quick Start

### Basic Usage
```bash
# Generate initial dataset (1000 rows each)
python synthetic_data_app.py \
    --table-names users,products,orders \
    --num-rows 1000 \
    --s3-bucket my-data-bucket

# Add more orders (with 50MB file size limit)
python synthetic_data_app.py \
    --table-names orders \
    --num-rows 5000 \
    --file-size-limit 50 \
    --s3-bucket my-data-bucket
```

### Command-Line Arguments

| Argument | Required | Description | Example |
|----------|----------|-------------|---------|
| `--table-names` | Yes | Comma-separated table names | `users,products,orders` |
| `--num-rows` | Yes | Number of rows per table | `1000` |
| `--s3-bucket` | Yes | S3 bucket for uploads | `my-data-bucket` |
| `--file-size-limit` | No | Max file size in MB | `50` |

## Data Schema

### Users Table (25+ fields)
- **Demographics**: Name, email, phone, address, date of birth
- **Account**: Registration date, status, verification, login history
- **Preferences**: Language, marketing opt-in, loyalty tier
- **Behavioral**: Customer segment, source channel, credit score range

### Products Table (25+ fields)
- **Basic Info**: Name, SKU, brand, category, description
- **Pricing**: Price, cost, profit margins
- **Physical**: Weight, dimensions, color, material, size
- **Inventory**: Stock levels, reorder points, supplier data
- **Performance**: Ratings, reviews, sales rank, tags

### Orders Table (30+ fields)
- **Identification**: Order number, tracking number
- **Relationships**: User ID (foreign key), product references
- **Financial**: Subtotal, tax, shipping, discounts, total
- **Status**: Order status, payment status, delivery dates
- **Fulfillment**: Addresses, shipping method, notes

## Usage Patterns

### 1. Initial Dataset Creation
```bash
# Start with base tables
python synthetic_data_app.py \
    --table-names users,products \
    --num-rows 1000 \
    --s3-bucket my-bucket

# Then add dependent tables
python synthetic_data_app.py \
    --table-names orders \
    --num-rows 2000 \
    --s3-bucket my-bucket
```

### 2. Incremental Data Addition
```bash
# Add more users and orders
python synthetic_data_app.py \
    --table-names users,orders \
    --num-rows 500 \
    --s3-bucket my-bucket
```

### 3. Large Dataset Generation
```bash
# Generate large dataset with chunking
python synthetic_data_app.py \
    --table-names orders \
    --num-rows 100000 \
    --file-size-limit 25 \
    --s3-bucket my-bucket
```

## File Organization

### S3 Structure
```
my-bucket/
├── users/
│   ├── 202505301430.parquet
│   └── 202505301500.parquet
├── products/
│   ├── 202505301430.parquet
│   └── 202505301500.parquet
└── orders/
    ├── 202505301430_part_001.parquet
    ├── 202505301430_part_002.parquet
    └── 202505301500.parquet
```

### Local Files
```
project-directory/
├── metadata.json              # ID tracking and relationships
├── synthetic_data_app.py      # Main application
├── data_generators.py         # Table-specific generators
├── metadata_manager.py        # Metadata persistence
└── s3_uploader.py            # S3 upload management
```

## Architecture

### Core Components

1. **MetadataManager**: Tracks IDs and relationships across runs
2. **DataGenerator**: Base class with dynamic method dispatch
3. **UserDataGenerator**: Creates realistic user demographics
4. **ProductDataGenerator**: Generates product catalog data
5. **OrderDataGenerator**: Creates transaction data with foreign keys
6. **S3Uploader**: Handles intelligent chunking and upload
7. **SyntheticDataApplication**: Orchestrates the entire pipeline

### Data Flow
```
Command Line Args → Application → Metadata Loading → 
Data Generation → S3 Upload → Metadata Persistence
```

## Advanced Configuration

### Custom Table Generators
Extend the `DataGenerator` base class to add new table types:

```python
class CustomTableGenerator(DataGenerator):
    def generate_custom_table(self, num_rows: int) -> pd.DataFrame:
        # Implementation here
        pass
```

### Metadata Customization
Modify metadata structure for additional table types:

```python
# In MetadataManager._load_metadata()
return {
    "users": {"last_id": 0, "existing_ids": []},
    "products": {"last_id": 0, "existing_ids": []},
    "orders": {"last_id": 0, "existing_ids": []},
    "custom_table": {"last_id": 0, "existing_ids": []}
}
```

## Data Quality Features

### Realistic Distributions
- Weighted random choices for categorical data
- Proper demographic distributions
- Realistic business metrics and ratios

### Data Integrity
- Foreign key relationships maintained across runs
- No orphaned records or invalid references
- Consistent data types and formats

### Time Handling
- All timestamps in UTC format
- Logical date relationships (registration before last login)
- Age validation (18+ years for users)

## Performance Considerations

### Memory Usage
- Processes data in configurable chunks
- Temporary file cleanup prevents disk bloat
- Sample-based size estimation minimizes memory overhead

### File Size Management
- Intelligent chunking based on actual compressed sizes
- Configurable limits prevent oversized files
- Parquet format provides optimal compression

### S3 Optimization
- Organized folder structure for efficient querying
- Timestamped naming prevents conflicts
- Multi-part uploads for large datasets

## Troubleshooting

### Common Issues

**Error: "No existing users found"**
```bash
# Solution: Generate users first
python synthetic_data_app.py --table-names users --num-rows 100 --s3-bucket bucket
```

**AWS Credentials Error**
```bash
# Configure AWS credentials
aws configure
# Or set environment variables
export AWS_ACCESS_KEY_ID=your_access_key
export AWS_SECRET_ACCESS_KEY=your_secret_key
```

**Memory Issues with Large Datasets**
```bash
# Use smaller chunks
python synthetic_data_app.py --file-size-limit 10 ...
```

### Debugging
- Check `metadata.json` for current state
- Verify S3 bucket permissions
- Review AWS CloudTrail for upload issues

## Use Cases

### Development & Testing
- Populate development databases
- Test application performance with realistic data
- Validate data processing pipelines

### Analytics & BI
- Create datasets for dashboard development
- Test analytical queries and performance
- Train machine learning models

### Data Engineering
- Validate ETL pipeline performance
- Test data warehouse loading procedures
- Benchmark query optimization

## Contributing

1. Fork the repository
2. Create a feature branch
3. Add comprehensive tests for new functionality
4. Ensure all docstrings follow the established format
5. Maintain line length limits (77 characters)
6. Submit a pull request with detailed description

## License

[Specify your license here]

## Support

For issues, questions, or feature requests:
- Create an issue in the repository
- Check existing documentation and troubleshooting guides
- Review code comments for implementation details

---

**Note**: This project generates synthetic data for testing and development purposes. Ensure compliance with your organization's data policies and any applicable regulations when using generated datasets.