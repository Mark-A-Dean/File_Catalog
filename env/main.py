from pathlib import Path
import file_details as fd 

SOURCE_URN = Path(r"A:Alice\Bob")

if __name__ == "__main__":
    df = fd.process_file_collection(SOURCE_URN)
    print(df)
