"""
Excel to MySQL Database Upload - Example Usage Script

This script demonstrates how to upload Excel files to MySQL database
using the insert_database function from test.ipynb

Prerequisites:
1. Install required packages: pip install pandas openpyxl mysql-connector-python pymysql python-dotenv
2. Configure .env file with MySQL credentials
3. Prepare your Excel file

Author: Auto-generated example
Date: 2025-12-02
"""

import os
import pandas as pd
import pymysql
from datetime import datetime
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# MySQL Configuration from .env file
MYSQL_CONFIG = {
    'host': os.getenv('MYSQL_HOST'),
    'user': os.getenv('MYSQL_USER'),
    'password': os.getenv('MYSQL_PASSWORD'),
    'database': os.getenv('MYSQL_DATABASE'),
    'port': int(os.getenv('MYSQL_PORT'))
}


def safe_convert_for_mysql(df):
    """
    Convert all columns to types compatible with MySQL.
    """
    for column in df.columns:
        try:
            # Get sample of non-null values
            sample = df[column].dropna().head(1)
            if len(sample) == 0:
                df[column] = df[column].astype(str)
                continue

            # Get column data type
            dtype = df[column].dtype
            sample_value = sample.iloc[0]

            # Handle different data types
            if pd.api.types.is_float_dtype(dtype):
                if df[column].dropna().apply(lambda x: x.is_integer()).all():
                    df[column] = df[column].astype('Int64')  # Nullable integer
                else:
                    # Keep as float for MySQL DOUBLE type
                    pass

            elif isinstance(dtype, pd.CategoricalDtype):
                df[column] = df[column].astype(str)

            elif pd.api.types.is_object_dtype(dtype):
                # Handle lists, dicts, sets, tuples
                if isinstance(sample_value, (list, dict, set, tuple)):
                    df[column] = df[column].apply(lambda x: str(x) if x is not None else None)

                # Convert to datetime if applicable
                try:
                    converted_col = pd.to_datetime(df[column], format="%Y-%m-%d %H:%M:%S", errors='coerce')
                    if converted_col.notna().sum() > 0:  # Check if valid datetime values exist
                        df[column] = converted_col.dt.strftime('%Y-%m-%d %H:%M:%S')
                    else:
                        df[column] = df[column].astype(str)  # Keep as string if all conversions failed
                except Exception:
                    df[column] = df[column].astype(str)

            elif pd.api.types.is_datetime64_dtype(dtype):
                df[column] = df[column].dt.strftime('%Y-%m-%d %H:%M:%S')

            elif pd.api.types.is_timedelta64_dtype(dtype):
                df[column] = df[column].apply(lambda x: str(x.total_seconds()) if pd.notnull(x) else None)

            elif pd.api.types.is_bool_dtype(dtype):
                # MySQL uses TINYINT(1) for boolean
                pass

            elif pd.api.types.is_integer_dtype(dtype):
                pass  # Keep integer as is

            else:
                df[column] = df[column].astype(str)

        except Exception as e:
            print(f"Warning: Error converting column {column}. Converting to string. Error: {str(e)}")
            df[column] = df[column].astype(str)

    return df


def get_mysql_type(pandas_dtype, column_values):
    """
    Map pandas dtypes to MySQL data types.
    """
    if pd.api.types.is_datetime64_dtype(pandas_dtype):
        return "DATETIME"
    elif pd.api.types.is_bool_dtype(pandas_dtype):
        return "TINYINT(1)"
    elif pd.api.types.is_integer_dtype(pandas_dtype):
        max_val = column_values.max() if not column_values.empty else 0
        # Choose appropriate integer type based on size
        if max_val <= 127:
            return "TINYINT"
        elif max_val <= 32767:
            return "SMALLINT"
        elif max_val <= 8388607:
            return "MEDIUMINT"
        elif max_val <= 2147483647:
            return "INT"
        else:
            return "BIGINT"
    elif pd.api.types.is_float_dtype(pandas_dtype):
        return "DOUBLE"
    else:
        # For strings, determine max length to choose VARCHAR or TEXT
        non_null_values = column_values.dropna()
        if len(non_null_values) > 0:
            max_length = non_null_values.astype(str).str.len().max()
            if max_length <= 65535:
                return "TEXT"
            elif max_length <= 16777215:
                return "MEDIUMTEXT"
            else:
                return "LONGTEXT"
        return "VARCHAR(255)"  # Default


def create_mysql_connection():
    """
    Create and return a MySQL connection.
    """
    try:
        connection = pymysql.connect(
            host=os.getenv('MYSQL_HOST'),
            user=os.getenv('MYSQL_USER'),
            password=os.getenv('MYSQL_PASSWORD'),
            db=os.getenv('MYSQL_DATABASE'),
            port=int(os.getenv('MYSQL_PORT')),
        )

        if connection.open:
            print("✓ Connection to MySQL database successful")
            return connection
    except Exception as e:
        print(f"✗ Error connecting to MySQL: {e}")
        return None


def insert_database(table_name, data_frame):
    """
    Insert dataframe into MySQL table with dynamic schema creation.
    
    Args:
        table_name (str): Name of the table to insert data into
        data_frame (pd.DataFrame): DataFrame containing the data to insert
        
    Returns:
        str: Success or error message
    """
    try:
        # Create a copy to avoid modifying the original DataFrame
        df = data_frame.copy()

        # Clean unnamed columns
        df.drop(
            df.columns[df.columns.str.contains('unnamed', case=False)],
            axis=1, inplace=True
        )

        # Convert all columns to MySQL-compatible types
        df = safe_convert_for_mysql(df)

        # Create MySQL connection
        connection = create_mysql_connection()
        if not connection:
            return "Failed to connect to MySQL database"

        cursor = connection.cursor()

        # Check if table exists
        cursor.execute(f"SHOW TABLES LIKE '{table_name}'")
        table_exists = cursor.fetchone()

        if not table_exists:
            # Create table with schema
            column_definitions = []
            for col in df.columns:
                mysql_type = get_mysql_type(df[col].dtype, df[col])
                # MySQL doesn't like certain characters in column names
                col_name = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                column_definitions.append(f"`{col_name}` {mysql_type}")

            # Add an auto-incrementing ID column and created_at timestamp
            create_table_sql = f"""
            CREATE TABLE `{table_name}` (
                `id` INT AUTO_INCREMENT PRIMARY KEY,
                {', '.join(column_definitions)},
                `created_at` TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
            """
            cursor.execute(create_table_sql)
            connection.commit()
            print(f"✓ Table '{table_name}' created successfully")
        else:
            # Table exists, check if we need to add new columns
            cursor.execute(f"DESCRIBE `{table_name}`")
            existing_columns = {row[0] for row in cursor.fetchall()}
            
            new_columns_added = 0
            for col in df.columns:
                # MySQL doesn't like certain characters in column names
                col_name = col.replace(' ', '_').replace('-', '_').replace('.', '_')
                if col_name not in existing_columns and col_name != 'id' and col_name != 'created_at':
                    mysql_type = get_mysql_type(df[col].dtype, df[col])
                    alter_table_sql = f"ALTER TABLE `{table_name}` ADD COLUMN `{col_name}` {mysql_type}"
                    cursor.execute(alter_table_sql)
                    connection.commit()
                    new_columns_added += 1
            
            if new_columns_added > 0:
                print(f"✓ Added {new_columns_added} new column(s) to table '{table_name}'")

        # Insert data in batches
        batch_size = 1000  # Adjust based on your needs
        total_rows = len(df)
        
        for start_idx in range(0, total_rows, batch_size):
            end_idx = min(start_idx + batch_size, total_rows)
            batch_df = df.iloc[start_idx:end_idx]
            
            # Generate placeholders and values for the insert query
            placeholders = ', '.join(['%s'] * len(batch_df.columns))
            columns = [col.replace(' ', '_').replace('-', '_').replace('.', '_') for col in batch_df.columns]
            columns_str = ', '.join([f'`{col}`' for col in columns])
            
            insert_sql = f"INSERT INTO `{table_name}` ({columns_str}) VALUES ({placeholders})"
            
            # Prepare data for insertion
            data_values = [tuple(row) for row in batch_df.values]
            
            # Execute insert
            cursor.executemany(insert_sql, data_values)
            connection.commit()
            
            print(f"  → Inserted batch {start_idx + 1}-{end_idx} of {total_rows} rows")

        cursor.close()
        connection.close()
        return f"✓ Successfully inserted {len(df)} rows into table '{table_name}'"

    except Exception as e:
        print(f"✗ Error: {str(e)}")
        return str(e)


# ============================================================================
# EXAMPLE USAGE FUNCTIONS
# ============================================================================

def upload_single_excel(excel_file_path, table_name=None):
    """
    Upload a single Excel file to MySQL database.
    
    Args:
        excel_file_path (str): Path to the Excel file
        table_name (str, optional): Name of the table. If None, uses filename
        
    Example:
        upload_single_excel('data/products.xlsx', 'products_table')
        upload_single_excel('sales_data.xlsx')  # Table name will be 'sales_data'
    """
    try:
        # Check if file exists
        if not os.path.exists(excel_file_path):
            print(f"✗ Error: File '{excel_file_path}' not found!")
            return
        
        # Generate table name from filename if not provided
        if table_name is None:
            table_name = os.path.splitext(os.path.basename(excel_file_path))[0]
            # Clean table name for MySQL
            table_name = table_name.replace('.', '_').replace('-', '_').replace(' ', '_')
        
        print(f"\n{'='*60}")
        print(f"Processing: {excel_file_path}")
        print(f"Table name: {table_name}")
        print(f"{'='*60}\n")
        
        # Read Excel file
        print("Reading Excel file...")
        df = pd.read_excel(excel_file_path)
        print(f"✓ Loaded {len(df)} rows and {len(df.columns)} columns")
        print(f"  Columns: {', '.join(df.columns.tolist())}\n")
        
        # Insert into database
        print("Inserting data into MySQL...")
        result = insert_database(table_name, df)
        print(f"\n{result}\n")
        
    except Exception as e:
        print(f"✗ Error processing file: {e}")


def upload_multiple_excel(excel_files_dict):
    """
    Upload multiple Excel files to MySQL database.
    
    Args:
        excel_files_dict (dict): Dictionary with {table_name: excel_file_path}
        
    Example:
        files = {
            'products': 'data/products.xlsx',
            'customers': 'data/customers.xlsx',
            'orders': 'data/orders.xlsx'
        }
        upload_multiple_excel(files)
    """
    total_files = len(excel_files_dict)
    successful = 0
    failed = 0
    
    print(f"\n{'='*60}")
    print(f"BATCH UPLOAD: {total_files} files")
    print(f"{'='*60}\n")
    
    for idx, (table_name, excel_file) in enumerate(excel_files_dict.items(), 1):
        print(f"\n[{idx}/{total_files}] Processing: {excel_file} → {table_name}")
        print("-" * 60)
        
        try:
            if not os.path.exists(excel_file):
                print(f"✗ File not found: {excel_file}")
                failed += 1
                continue
            
            df = pd.read_excel(excel_file)
            print(f"✓ Loaded {len(df)} rows")
            
            result = insert_database(table_name, df)
            print(result)
            successful += 1
            
        except Exception as e:
            print(f"✗ Error: {e}")
            failed += 1
    
    print(f"\n{'='*60}")
    print(f"BATCH UPLOAD COMPLETE")
    print(f"  Successful: {successful}/{total_files}")
    print(f"  Failed: {failed}/{total_files}")
    print(f"{'='*60}\n")


def upload_excel_with_custom_table_name(excel_file_path, table_name, sheet_name=0):
    """
    Upload specific sheet from Excel file with custom table name.
    
    Args:
        excel_file_path (str): Path to the Excel file
        table_name (str): Custom table name
        sheet_name (str or int): Sheet name or index (default: 0)
        
    Example:
        upload_excel_with_custom_table_name('report.xlsx', 'monthly_sales', 'January')
        upload_excel_with_custom_table_name('report.xlsx', 'quarterly_data', 1)
    """
    try:
        print(f"\n{'='*60}")
        print(f"Processing: {excel_file_path}")
        print(f"Sheet: {sheet_name}")
        print(f"Table: {table_name}")
        print(f"{'='*60}\n")
        
        # Read specific sheet
        df = pd.read_excel(excel_file_path, sheet_name=sheet_name)
        print(f"✓ Loaded {len(df)} rows from sheet '{sheet_name}'")
        
        # Insert into database
        result = insert_database(table_name, df)
        print(f"\n{result}\n")
        
    except Exception as e:
        print(f"✗ Error: {e}")


# ============================================================================
# MAIN EXECUTION EXAMPLES
# ============================================================================

if __name__ == "__main__":
    print("\n" + "="*60)
    print("EXCEL TO MYSQL DATABASE UPLOADER")
    print("="*60)
    
    # ========================================================================
    # EXAMPLE 1: Upload a single Excel file
    # ========================================================================
    print("\n### EXAMPLE 1: Upload Single Excel File ###\n")
    
    # Replace with your actual Excel file path
    # upload_single_excel('your_file.xlsx', 'your_table_name')
    
    # Example with automatic table naming
    # upload_single_excel('products.xlsx')  # Creates table 'products'
    
    
    # ========================================================================
    # EXAMPLE 2: Upload multiple Excel files
    # ========================================================================
    print("\n### EXAMPLE 2: Upload Multiple Excel Files ###\n")
    
    # files_to_upload = {
    #     'products_table': 'data/products.xlsx',
    #     'customers_table': 'data/customers.xlsx',
    #     'orders_table': 'data/orders.xlsx'
    # }
    # upload_multiple_excel(files_to_upload)
    
    
    # ========================================================================
    # EXAMPLE 3: Upload specific sheet with custom table name
    # ========================================================================
    print("\n### EXAMPLE 3: Upload Specific Sheet ###\n")
    
    # upload_excel_with_custom_table_name('report.xlsx', 'january_sales', 'January')
    
    
    # ========================================================================
    # EXAMPLE 4: Quick test with sample data
    # ========================================================================
    print("\n### EXAMPLE 4: Test with Sample Data ###\n")
    
    # Uncomment to create and upload sample data
    """
    # Create sample Excel file
    sample_data = {
        'Name': ['John Doe', 'Jane Smith', 'Bob Johnson'],
        'Age': [30, 25, 35],
        'Email': ['john@example.com', 'jane@example.com', 'bob@example.com'],
        'Salary': [50000.50, 60000.75, 55000.25],
        'Join_Date': pd.to_datetime(['2023-01-15', '2023-02-20', '2023-03-10'])
    }
    
    sample_df = pd.DataFrame(sample_data)
    sample_df.to_excel('sample_employees.xlsx', index=False)
    print("✓ Created sample file: sample_employees.xlsx")
    
    # Upload to database
    upload_single_excel('sample_employees.xlsx', 'test_employees')
    """
    
    
    # ========================================================================
    # INTERACTIVE MODE
    # ========================================================================
    print("\n### INTERACTIVE MODE ###\n")
    
    choice = input("Do you want to upload an Excel file now? (yes/no): ").strip().lower()
    
    if choice in ['yes', 'y']:
        file_path = input("\nEnter the path to your Excel file: ").strip()
        
        use_custom_name = input("Use custom table name? (yes/no): ").strip().lower()
        
        if use_custom_name in ['yes', 'y']:
            table_name = input("Enter table name: ").strip()
            upload_single_excel(file_path, table_name)
        else:
            upload_single_excel(file_path)
    else:
        print("\nNo file uploaded. Uncomment the examples above to test!")
        print("\nQuick Start Guide:")
        print("1. Prepare your Excel file")
        print("2. Update the file path in the examples")
        print("3. Uncomment the example you want to run")
        print("4. Run this script: python excel_to_db_example.py")
        print("\nOr use interactive mode by answering 'yes' when prompted!")
