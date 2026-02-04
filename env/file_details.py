from datetime import datetime, timezone
from multiprocessing import Pool, cpu_count
from pathlib import Path
from typing import Any, Callable, Dict, Generator, List, Optional, Tuple
import hashlib
import pandas as pd
import re

def row_generator(dataset: List[Tuple]) -> Generator[Dict[str, Any], None, None]:
    for o in dataset:
        yield {
            "name": o[0],
            "extension": o[1],
            "parent": o[2],
            "date_created": o[3],
            "date_last_accessed": o[4],
            "date_last_modified": o[5],
            "object_type": o[6],
            "URI": o[7],
            "hash_id": o[8]
        }

def get_file_collection(path: Path) -> List[Path] | None:
    if not path.is_dir():
        raise TypeError(f"\"{path}\" is not a valid directory.").with_traceback(None)
    
    class FileCollectionWrapper(list):
        def __init__(self, files, pattern = r"\.\w+?$"):
            super().__init__(files)
            self.__pattern = pattern

        def __bool__(self):
            return any(re.search(self.__pattern, str(mbr)) for mbr in self)

        def print_summary(self):
            print(f"Total items: {len(self)}")
            print(f"Extensions found: {set(f.suffix for f in self)}")

    return FileCollectionWrapper(
        Path(path).rglob('*')
    )

def process_file_collection(source_path: Path):
    try:
        file_collection = get_file_collection(path=source_path)
    
        with Pool() as pool:
            nbr_procs = Pool(cpu_count())
            df = pd.DataFrame(
                row_generator(
                    nbr_procs.imap_unordered(
                        FileDetails.get_file_details,
                        file_collection,
                        chunksize = 1000
                    )
                )
            ).sort_values(["URI"])
        return df
    except (TypeError, PermissionError) as hn:
        print(f"There was an error during the processing of the file collection:\n\t{hn}")
        return None

class FileDetails:
    @staticmethod
    def format_datetime_utc(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    
    @staticmethod
    def obj_type_detector(path: Path) -> str:
        return "Directory" if path.is_dir() else "File"
    
    @classmethod
    def get_file_details(cls, i: Path, 
        format_datetime_utc: Optional[Callable[[float], str]] = None,
        obj_type_detector: Optional[Callable[[Path], str]] = None
    ) -> Tuple:

        file_stats = i.stat()        
        format_datetime_utc = format_datetime_utc or cls.format_datetime_utc
        obj_type_detector = obj_type_detector or cls.obj_type_detector
        uri = i.as_uri()
        hash_id = hashlib.sha256(uri.encode()).hexdigest()

        return (
            i.name,
            i.suffix,
            i.parent.stem,
            format_datetime_utc(file_stats.st_birthtime if hasattr(file_stats, 'st_birthtime') else file_stats.st_birthtime),
            format_datetime_utc(file_stats.st_atime),
            format_datetime_utc(file_stats.st_mtime),
            obj_type_detector(i),
            uri,
            hash_id
        )
