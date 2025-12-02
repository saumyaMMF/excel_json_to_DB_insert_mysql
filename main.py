# Cell 1: Import functions
import os
from upload_functions import (
    upload_single_json,
    upload_all_json_from_folder,
    upload_single_excel,
    upload_all_excel_from_folder,
    upload_dataframe,
    delete_data_by_date
)

def main():
    while True:
        print("\n" + "="*40)
        print("   EXCEL/JSON TO DATABASE UPLOADER")
        print("="*40)
        print("1. Upload Single Excel File")
        print("2. Upload All Excel Files from Folder")
        print("3. Upload Single JSON File")
        print("4. Upload All JSON Files from Folder")
        print("5. Upload DataFrame (Internal Use)")
        print("6. Delete Data by Date")
        print("7. Exit")
        print("-" * 40)
        
        choice = input("Enter your choice (1-7): ").strip()
        
        if choice == '1':
            file_path = input("Enter path to Excel file: ").strip().strip('"')
            table_name = input("Enter custom table name (or press Enter for default): ").strip()
            upload_single_excel(file_path, table_name if table_name else None)
            
        elif choice == '2':
            folder_path = input("Enter path to folder containing Excel files: ").strip().strip('"')
            upload_all_excel_from_folder(folder_path)
            
        elif choice == '3':
            file_path = input("Enter path to JSON file: ").strip().strip('"')
            table_name = input("Enter custom table name (or press Enter for default): ").strip()
            upload_single_json(file_path, table_name if table_name else None)
            
        elif choice == '4':
            folder_path = input("Enter path to folder containing JSON files: ").strip().strip('"')
            upload_all_json_from_folder(folder_path)

        elif choice == '5':
             print("This option is for internal use or script integration.")
            
        elif choice == '6':
            table_name = input("Enter table name: ").strip()
            date_value = input("Enter date (YYYY-MM-DD): ").strip()
            date_col = input("Enter date column name (press Enter for default 'date'): ").strip()
            
            print(f"\nDeleting data from '{table_name}' for date '{date_value}'...")
            result = delete_data_by_date(table_name, date_value, date_col if date_col else 'date')
            print(f"\n{result}\n")
            
        elif choice == '7':
            print("Exiting...")
            break
            
        else:
            print("Invalid choice. Please try again.")
            
        input("\nPress Enter to continue...")

if __name__ == "__main__":
    main()