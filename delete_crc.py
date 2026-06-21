import os

def main():
    model_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "training_model", "spark_rf_model")
    print(f"Searching for all .crc files (including hidden ones) in: {model_dir}")
    
    crc_files = []
    for root, dirs, files in os.walk(model_dir):
        for file in files:
            if file.endswith(".crc"):
                crc_files.append(os.path.join(root, file))
                
    if not crc_files:
        print("No .crc files found.")
        return
        
    print(f"Found {len(crc_files)} .crc files to delete.")
    for f in crc_files:
        try:
            os.remove(f)
            print(f"Deleted: {f}")
        except Exception as e:
            print(f"Failed to delete {f}: {e}")
            
    print("Done cleaning .crc files.")

if __name__ == "__main__":
    main()
