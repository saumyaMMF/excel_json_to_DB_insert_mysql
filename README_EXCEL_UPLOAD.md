# Excel to MySQL Database Upload Guide

## 📋 Overview

This guide shows you how to upload Excel files to your MySQL database using the provided script.

## 🚀 Quick Start

### 1. Prerequisites

Make sure you have the required packages installed:

```bash
pip install pandas openpyxl mysql-connector-python pymysql python-dotenv
```

### 2. Configure Database Connection

Create or update your `.env` file with MySQL credentials:

```env
MYSQL_HOST=your_host
MYSQL_USER=your_username
MYSQL_PASSWORD=your_password
MYSQL_DATABASE=your_database
MYSQL_PORT=3306
```

### 3. Run the Script

```bash
python excel_to_db_example.py
```

## 💡 Usage Examples

### Example 1: Upload a Single Excel File

```python
from excel_to_db_example import upload_single_excel

# Automatic table naming (uses filename)
upload_single_excel('products.xlsx')

# Custom table name
upload_single_excel('products.xlsx', 'my_products_table')
```

### Example 2: Upload Multiple Excel Files

```python
from excel_to_db_example import upload_multiple_excel

files = {
    'products_table': 'data/products.xlsx',
    'customers_table': 'data/customers.xlsx',
    'orders_table': 'data/orders.xlsx'
}

upload_multiple_excel(files)
```

### Example 3: Upload Specific Sheet

```python
from excel_to_db_example import upload_excel_with_custom_table_name

# Upload sheet by name
upload_excel_with_custom_table_name('report.xlsx', 'january_sales', 'January')

# Upload sheet by index (0 = first sheet)
upload_excel_with_custom_table_name('report.xlsx', 'quarterly_data', 0)
```

### Example 4: Interactive Mode

Simply run the script and follow the prompts:

```bash
python excel_to_db_example.py
```

Then answer the questions:
- Do you want to upload an Excel file now? **yes**
- Enter the path to your Excel file: **data/myfile.xlsx**
- Use custom table name? **yes**
- Enter table name: **my_custom_table**

## 📊 What Happens When You Upload?

1. **File Reading**: The script reads your Excel file using pandas
2. **Data Type Conversion**: Automatically converts data types to MySQL-compatible formats
3. **Table Creation**: 
   - If table doesn't exist, it creates one with appropriate column types
   - If table exists, it adds any new columns found in your Excel
4. **Data Insertion**: Inserts data in batches (1000 rows at a time) for efficiency
5. **Auto-generated Columns**: Adds `id` (auto-increment) and `created_at` (timestamp)

## 🔧 Features

✅ **Automatic Schema Detection**: Analyzes your Excel columns and creates appropriate MySQL column types

✅ **Dynamic Table Creation**: Creates tables automatically if they don't exist

✅ **Column Addition**: Adds new columns to existing tables when needed

✅ **Batch Processing**: Handles large files efficiently with batch inserts

✅ **Data Type Handling**: Supports integers, floats, strings, dates, booleans, and more

✅ **Error Handling**: Provides clear error messages and progress updates

## 📝 Sample Data Test

Want to test with sample data? Uncomment this section in the script:

```python
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

# Upload to database
upload_single_excel('sample_employees.xlsx', 'test_employees')
```

## 🎯 Real-World Example

Let's say you have a file `sales_2024.xlsx` with columns:
- Product Name
- Quantity
- Price
- Sale Date

```python
from excel_to_db_example import upload_single_excel

# Upload the file
upload_single_excel('sales_2024.xlsx', 'sales_data')
```

This will:
1. Create a table called `sales_data` (if it doesn't exist)
2. Create columns: `Product_Name`, `Quantity`, `Price`, `Sale_Date`
3. Add auto-generated columns: `id`, `created_at`
4. Insert all rows from your Excel file

## ⚠️ Important Notes

- **Column Names**: Spaces and special characters in column names are replaced with underscores
- **Unnamed Columns**: Columns named "Unnamed" are automatically removed
- **Data Types**: The script intelligently detects and converts data types
- **Existing Tables**: If a table exists, new data is appended (not replaced)

## 🐛 Troubleshooting

### Connection Error
```
✗ Error connecting to MySQL: ...
```
**Solution**: Check your `.env` file credentials

### File Not Found
```
✗ Error: File 'myfile.xlsx' not found!
```
**Solution**: Verify the file path is correct

### Permission Error
```
✗ MySQL Error: Access denied...
```
**Solution**: Ensure your MySQL user has CREATE and INSERT privileges

## 📞 Need Help?

The script provides detailed progress messages:
- ✓ Success messages in green
- ✗ Error messages in red
- → Progress indicators for batch uploads

Watch the console output to track your upload progress!

## 🔄 Workflow Summary

```
Excel File → Read Data → Convert Types → Create/Update Table → Insert Data → Success!
```

---

**Happy Uploading! 🎉**
