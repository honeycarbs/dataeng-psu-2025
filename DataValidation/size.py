import os

def get_size(file_path):
    total_bytes = 0
    record_count = 0
    
    try:
        with open(file_path, 'rb') as file:
            file.seek(0, os.SEEK_END)
            total_bytes = file.tell()
            
    except FileNotFoundError:
        print(f"Error: File '{file_path}' not found.")
        return None
    except Exception as e:
        print(f"Error processing file: {e}")
        return None
    
    with open(file_path, 'r', encoding='utf-8') as file:
        record_count = sum(1 for line in file)
    
    return total_bytes, record_count