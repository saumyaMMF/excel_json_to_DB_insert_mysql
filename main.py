# Cell 1: Import functions
from upload_functions import (
    upload_single_json,
    upload_all_json_from_folder,
    upload_single_excel,
    upload_all_excel_from_folder
)


# # Cell 2: Upload all JSON files from data_base folder
# folder_path = r'.\database_files'
# upload_all_json_from_folder(folder_path)


# Cell 3: Or upload a single file
upload_single_json(
    r'.\database_files\db_31northvt.json',
)