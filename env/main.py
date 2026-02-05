from pathlib import Path
import file_details as fd 
import os
from contextlib import contextmanager
import time

@contextmanager
def timer():
    start_time = time.perf_counter()
    yield
    end_time = time.perf_counter()
    print(f"Time elapsed: {format_seconds(end_time-start_time)}")

def format_seconds(seconds: float)-> str:
    hr = int(seconds//3600)
    min = int((seconds %3600) // 60)
    s = seconds % 60
    ms = int(seconds * 1000)
    return f"{hr:02}:{min:02}:{int(s):02}.{ms:03}"

SOURCE_URI: list[Path] = [
    Path(r"A:\alice"),
    Path(r"B:\bob"),
    Path(r"C:\charlie") #size:gigantic
]

if __name__ == "__main__":
    os.system("clear")
    with timer():
        df = fd.process_file_collection(SOURCE_URI[2])
        print(df,end="\n\n")
