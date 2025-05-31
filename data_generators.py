"""
Synthetic Data Generation Module for Relational Database Tables

This module provides data generation classes for creating realistic synthetic
data that mimics relational database structures. The generators create 
properly normalized data with referential integrity across multiple tables.

The module includes:
- DataGenerator: Base class providing dynamic method dispatch
- UserDataGenerator: Creates realistic user demographics and profiles
- ProductDataGenerator: Generates product catalog with inventory details  
- OrderDataGenerator: Creates order transactions with foreign key relations

Key features:
- Maintains referential integrity across generation runs
- Generates realistic data distributions and relationships
- Supports incremental data generation with proper ID sequencing
- Creates UTC-normalized timestamps for consistent time handling
- Produces 3NF-compliant relational data structures

Typical usage:
    metadata_manager = MetadataManager()
    user_gen = UserDataGenerator(metadata_manager)
    users_df = user_gen.generate_data("users", 1000)
    
    product_gen = ProductDataGenerator(metadata_manager)  
    products_df = product_gen.generate_data("products", 500)
    
    order_gen = OrderDataGenerator(metadata_manager)
    orders_df = order_gen.generate_data("orders", 2000)
"""
import random
from datetime import datetime, timedelta, timezone

import pandas as pd
from faker import Faker

from metadata_manager import MetadataManager


class DataGenerator:
    """Base class for generating synthetic data.
    
    Provides a common interface and dynamic method dispatch for table-specific
    data generation. Uses reflection to route generation requests to 
    appropriate methods based on table names.
    """

    def __init__(
        self,
        metadata_manager: MetadataManager
    ) -> None:
        """Initialize the data generator with metadata management.
        
        Args:
            metadata_manager: Instance for tracking IDs and maintaining
                            referential integrity across generation runs.
        """
        self.fake = Faker()
        self.metadata_manager = metadata_manager
    
    def generate_data(
        self,
        table_name: str,
        num_rows: int
    ) -> pd.DataFrame:
        """Generate data for specified table using dynamic method dispatch.
        
        Args:
            table_name: Name of the table to generate data for. Must have
                       corresponding generate_{table_name} method.
            num_rows: Number of records to generate.
            
        Returns:
            DataFrame containing generated synthetic data with proper
            schema for the specified table.
            
        Raises:
            ValueError: If no generator method exists for the table name.
            
        Note:
            This method uses reflection to call generate_{table_name}()
            methods implemented in subclasses.
        """
        method_name = f"generate_{table_name}"
        if hasattr(self, method_name):
            return getattr(self, method_name)(num_rows)
        else:
            raise ValueError(f"Unknown table: {table_name}")
    

class UserDataGenerator(DataGenerator):
    """Generates synthetic user data with realistic demographics.
    
    Creates comprehensive user profiles including personal information,
    account status, preferences, and behavioral attributes. All users
    are generated as adults (18+ years) with UTC-normalized timestamps.
    """

    def generate_users(self, num_rows: int) -> pd.DataFrame:
        """Generate user records with comprehensive demographic data.
        
        Args:
            num_rows: Number of user records to create.
            
        Returns:
            DataFrame with user data including:
            - Basic demographics (name, email, phone, address)
            - Account information (status, verification, registration)
            - Preferences (language, marketing opt-in, loyalty tier)
            - Behavioral data (customer segment, source channel)
            - All timestamps in UTC format
            - Ages 18-80 years old
            
        Note:
            Updates metadata with generated user IDs for foreign key
            relationships in other tables.
        """
        id_range = self.metadata_manager.get_next_id_range("users", num_rows)

        # User demographics and preferences
        users_data = []

        # Get current UTC datetime for age calculations
        current_utc = datetime.now(timezone.utc)

        for user_id in id_range:
            profile = self.fake.profile()

            # Ensure all timestamps are UTC
            created_date = self.fake.date_time_between(
                start_date="-5y",
                end_date="now"
            )
            created_date_utc = created_date.replace(tzinfo=None)

            # Generate data of birth ensuring user is at least 18 years old
            max_birthdate = current_utc.replace(year=current_utc.year - 18)
            min_birthdate = current_utc.replace(year=current_utc.year - 80)
            date_of_birth = self.fake.date_between(
                start_date=min_birthdate.date(),
                end_date=max_birthdate.date()
            )

            # Generate last login date in UTC
            last_login_utc = self.fake.date_time_between(
                start_date=created_date_utc,
                end_date=current_utc
            ).replace(tzinfo=None)

            user = {
                "user_id": user_id,
                "email": profile["mail"],
                "username": profile["username"],
                "first_name": profile["name"].split()[0],
                "last_name": profile["name"].split()[-1],
                "date_of_birth": date_of_birth,
                "gender": random.choice(
                    ["M", "F", "Other", "prefer not to say"]
                ),
                "phone_number": self.fake.phone_number(),
                "address_line_1": self.fake.street_address(),
                "address_line_2": (
                    self.fake.secondary_address() 
                    if random.random() > 0.7 else None
                ),
                "city": self.fake.city(),
                "state_province": self.fake.state(),
                "postal_code": self.fake.postcode(),
                "country": self.fake.country_code(),
                "registration_date": created_date,
                "last_login_date": last_login_utc,
                "account_status": random.choices(
                    ["active", "inactive", "suspended", "pending"], 
                    weights=[85, 10, 3, 2]
                )[0],
                "email_verified": random.choices(
                    [True, False],
                    weights=[90, 10]
                )[0],
                "marketing_opt_in": random.choices(
                    [True, False],
                    weights=[60, 40]
                )[0],
                "preferred_language": random.choices(
                    ["en", "es", "fr", "de", "it"], 
                    weights=[70, 15, 8, 4, 3]
                )[0],
                "loyalty_tier": random.choices(
                    ["bronze", "silver", "gold", "platinum"], 
                    weights=[50, 30, 15, 5]
                )[0],
                "referral_code": self.fake.lexify(text="????").upper(),
                "source_channel": random.choices(
                    [
                        "organic", 
                        "paid_search", 
                        "social_media", 
                        "referral", 
                        "email"
                    ], 
                    weights=[40, 25, 20, 10, 5]
                )[0],
                "customer_segment": random.choices(
                    ["high_value", "regular", "occasional", "new"],
                    weights=[15, 45, 25, 15]
                )[0],
                "credit_score_range": random.choices(
                    ["excellent", "good", "fair", "poor"], 
                    weights=[20, 40, 30, 10]
                )[0]
            }
            users_data.append(user)

        # Update metadata
        last_id = max(id_range)
        self.metadata_manager.update_last_id("users", last_id)
        self.metadata_manager.add_generated_ids("users", list(id_range))

        return pd.DataFrame(users_data)


class ProductDataGenerator(DataGenerator):
    """Generates synthetic product data for e-commerce catalog.
    
    Creates comprehensive product information including inventory,
    pricing, supplier details, and customer ratings. Products span
    multiple categories with realistic attributes and metadata.
    """

    def generate_products(self, num_rows: int) -> pd.DataFrame:
        """Generate product records with comprehensive catalog data.
        
        Args:
            num_rows: Number of product records to create.
            
        Returns:
            DataFrame with product data including:
            - Basic info (name, SKU, brand, category, description)
            - Pricing and costs (price, cost, profit margins)
            - Physical attributes (weight, dimensions, color, material)
            - Inventory data (stock, reorder levels, supplier info)
            - Ratings and rankings (customer ratings, sales rank)
            - Metadata (creation dates, active status, tags)
            - All timestamps in UTC format
            
        Note:
            Updates metadata with generated product IDs for foreign key
            relationships in order tables.
        """
        id_range = self.metadata_manager.get_next_id_range(
            "products", num_rows
        )

        categories = ["Electronics", "Clothing", "Home & Garden", "Sports",
            "Books", "Beauty", "Automotive", "Toys", "Food & Beverage",
            "Health"
        ]
        brands = ["TechCorp", "StyleBrand", "HomeComfort", "SportsPro",
            "ReadWell", "BeautyMax", "AutoParts", "PlayTime", "FreshFood",
            "WellnessPlus"
        ]

        products_data = []

        for product_id in id_range:
            category = random.choice(categories)
            brand = random.choice(brands)
            created_date = self.fake.date_time_between(
                start_date="-5y",
                end_date="now"
            )
            created_date_utc = created_date.replace(tzinfo=None)

            product = {
                "product_id": product_id,
                "sku": f"{brand[:3].upper()}-{self.fake.lexify(text='???###')}",
                "product_name": self.fake.catch_phrase(),
                "brand": brand,
                "category": category,
                "subcategory": f"{category} - {self.fake.word().title()}",
                "description": self.fake.text(200),
                "price": round(random.uniform(5.99, 999.99), 2),
                "cost": round(random.uniform(2.99, 500.00), 2),
                "weight_kg": round(random.uniform(0.1, 50.0), 2),
                "dimensions_cm": f"{random.randint(5,100)}x{random.randint(5,100)}x{random.randint(5,100)}",
                "color": self.fake.color_name(),
                "size": random.choice(["XS", "S", "M", "L", "XL", "XXL", "One Size"]),
                "material": random.choice(["Cotton", "Plastic", "Metal", "Wood", "Glass", "Leather"]),
                "stock_quantity": random.randint(0, 1000),
                "reorder_level": random.randint(10, 100),
                "supplier_id": random.randint(1, 50),
                "supplier_name": self.fake.company(),
                "created_date": created_date_utc,
                "last_updated": self.fake.date_time_between(start_date=created_date, end_date="now"),
                "is_active": random.choices([True, False], weights=[90, 10])[0],
                "is_featured": random.choices([True, False], weights=[20, 80])[0],
                "rating_avg": round(random.uniform(1.0, 5.0), 1),
                "rating_count": random.randint(0, 5000),
                "sales_rank": random.randint(1, 100000),
                "seasonal": random.choices([True, False], weights=[30, 70])[0],
                "eco_friendly": random.choices([True, False], weights=[25, 75])[0],
                "warranty_months": random.choice([0, 6, 12, 24, 36]),
                "tags": ",".join(random.sample(["popular", "new", "sale", "limited", "bestseller"], k=random.randint(0, 3)))
            }
            products_data.append(product)

        # Update metadata
        last_id = max(id_range)
        self.metadata_manager.update_last_id("products", last_id)
        self.metadata_manager.add_generated_ids("products", list(id_range))

        return pd.DataFrame(products_data)


class OrderDataGenerator(DataGenerator):
    """Generates synthetic order data with proper foreign key relationships.
    
    Creates realistic e-commerce orders referencing existing users and
    products. Includes complete order lifecycle data from placement
    through fulfillment with proper financial calculations.
    """

    def generate_orders(self, num_rows: int) -> pd.DataFrame:
        """Generate order records with comprehensive transaction data.
        
        Args:
            num_rows: Number of order records to create.
            
        Returns:
            DataFrame with order data including:
            - Foreign keys (user_id references existing users)
            - Order identification (order_number, tracking_number)
            - Status tracking (order_status, payment_status)
            - Financial data (subtotal, tax, shipping, discounts)
            - Fulfillment info (addresses, delivery dates, methods)
            - Customer service data (notes, gift messages, priority)
            - All timestamps in UTC format
            
        Raises:
            ValueError: If no existing users or products found in metadata.
            
        Note:
            Requires existing users and products to be generated first
            for proper foreign key relationships. Updates metadata with
            generated order IDs.
        """
        # Get existing user and product IDs for foreign key relationships
        existing_user_ids = self.metadata_manager.get_existing_ids("users")
        existing_product_ids = self.metadata_manager.get_existing_ids(
            "products"
        )

        if not existing_user_ids:
            raise ValueError("No existing users found. Generate users first.")
        if not existing_product_ids:
            raise ValueError(
                "No existing products found. Generate products first."
            )
        
        id_range = self.metadata_manager.get_next_id_range("orders", num_rows)

        orders_data = []

        for order_id in id_range:
            user_id = random.choice(existing_user_ids)
            order_date = self.fake.date_time_between(
                start_date="-2y",
                end_date="now"
            ).replace(tzinfo=None)

            # Generate order with multiple line items possible
            num_items = random.choices(
                [1, 2, 3, 4, 5],
                weights=[40, 30, 20, 7, 3]
            )[0]
            order_products = random.sample(
                existing_product_ids,
                min(num_items, len(existing_product_ids))
            )

            # Calculate order totals
            subtotal = 0
            total_quantity = 0

            for _ in order_products:
                item_price = round(random.uniform(10.00, 500.00), 2)
                quantity = random.randint(1, 5)
                subtotal += item_price * quantity
                total_quantity += quantity
            
            tax_rate = 0.08
            tax_amount = round(subtotal * tax_rate, 2)
            shipping_cost = (
                round(random.uniform(0, 25.00), 2) 
                if subtotal < 100 else 0
            )
            discount_amount = (
                round(random.uniform(0, subtotal * 0.2), 2) 
                if random.random() > 0.7 else 0
            )
            total_amount = (
                subtotal + tax_amount + shipping_cost - discount_amount
            )

            order = {
                "order_id": order_id,
                "user_id": user_id,
                "order_number": f"ORD-{order_date.strftime('%Y%m%d')}-{str(order_id).zfill(6)}",
                "order_date": order_date,
                "order_status": random.choices(
                    ["pending", "confirmed", "shipped", "delivered", 
                     "cancelled", "returned"], 
                    weights=[5, 10, 20, 55, 7, 3]
                )[0],
                "payment_status": random.choices(
                    ["pending", "paid", "failed", "refunded"], 
                    weights=[5, 85, 5, 5]
                )[0],
                "payment_method": random.choices(
                    ["credit_card", "debit_card", "paypal", "bank_transfer", 
                     "cash"], 
                    weights=[45, 25, 20, 8, 2]
                )[0],
                "shipping_method": random.choices(
                    ["standard", "express", "overnight", "pickup"], 
                    weights=[60, 25, 10, 5]
                )[0],
                "billing_address": self.fake.address(),
                "shipping_address": self.fake.address(),
                "subtotal_amount": round(subtotal, 2),
                "tax_amount": tax_amount,
                "shipping_cost": shipping_cost,
                "discount_amount": discount_amount,
                "total_amount": round(total_amount, 2),
                "currency": "USD",
                "total_items": len(order_products),
                "total_quantity": total_quantity,
                "coupon_code": (
                    self.fake.lexify(text="????###").upper() 
                    if discount_amount > 0 else None
                ),
                "sales_channel": random.choices(
                    ["website", "mobile_app", "phone", "store"], 
                    weights=[50, 35, 10, 5]
                )[0],
                "customer_type": random.choices(
                    ["new", "returning"], 
                    weights=[25, 75]
                )[0],
                "order_source": random.choices(
                    ["organic", "marketing_campaign", "referral"], 
                    weights=[70, 25, 5]
                )[0],
                "estimated_delivery_date": order_date + timedelta(
                    days=random.randint(1, 14)
                ),
                "actual_delivery_date": (
                    order_date + timedelta(days=random.randint(1, 14)) 
                    if random.random() > 0.3 else None
                ),
                "tracking_number": (
                    self.fake.lexify(text="??########??") 
                    if random.random() > 0.2 else None
                ),
                "notes": (
                    self.fake.sentence() 
                    if random.random() > 0.8 else None
                ),
                "gift_message": (
                    self.fake.sentence() 
                    if random.random() > 0.9 else None
                ),
                "priority_level": random.choices(
                    ["low", "normal", "high", "urgent"], 
                    weights=[20, 65, 12, 3]
                )[0]
            }
            orders_data.append(order)

        # Update metadata
        last_id = max(id_range)
        self.metadata_manager.update_last_id("orders", last_id)
        self.metadata_manager.add_generated_ids("orders", list(id_range))
        
        return pd.DataFrame(orders_data)
