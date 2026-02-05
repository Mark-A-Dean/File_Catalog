from datetime import datetime, timezone
from functools import lru_cache
from multiprocessing import Pool
from pathlib import Path
from typing import Any, Dict, Generator, List, Tuple
import hashlib
import pandas as pd
import re

def row_generator(dataset: List[Tuple]) -> Generator[Dict[str, Any], None, None]:
    for o in dataset:
        yield {
            "name": o[0],
            "extension": o[1],
            "size_KiB": o[2],
            "parent": o[3],
            "date_created": o[4],
            "date_last_accessed": o[5],
            "date_last_modified": o[6],
            "object_type": o[7],
            "URI": o[8],
            "hash_id": o[9]
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

        def close(self):
            pass #working on this; it might be used to clean up memory leaks

        def print_summary(self):
            print(f"Total items: {len(self)}")
            print(f"Extensions found: {set(f.suffix for f in self)}")

    return FileCollectionWrapper(Path(path).rglob('*'))

def process_file_collection(source_path: Path):
    try:
        file_collection = get_file_collection(path=source_path)
    
        with Pool(maxtasksperchild = 15) as pool:
            df = pd.DataFrame(
                row_generator(
                    pool.imap_unordered(
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
    MAX_PATH_LENGTH: int = 255

    @staticmethod
    @lru_cache(maxsize = None)
    def cached_stat(path: Path):
        return path.stat()

    @staticmethod
    def format_datetime_utc(timestamp: float) -> str:
        return datetime.fromtimestamp(timestamp, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S")
    
    @staticmethod
    def obj_type_detector(path: Path) -> str:
        return "Directory" if path.is_dir() else "File"
    
    @classmethod
    def get_file_details(cls, path: Path) -> Tuple:
        if len(str(path)) > cls.MAX_PATH_LENGTH:
            return (path.name, None, None, None, None, None, None, None, None, None)

        file_stats = cls.cached_stat(path)
        uri = path.as_uri()
        hash_id = hashlib.sha256(uri.encode()).hexdigest()

        return (
            path.name,
            path.suffix,
            round(path.stat().st_size / 1024,3) if path.is_file() else 0.000,
            path.parent.stem,
            cls.format_datetime_utc(file_stats.st_birthtime if hasattr(file_stats, 'st_birthtime') else file_stats.st_birthtime),
            cls.format_datetime_utc(file_stats.st_atime),
            cls.format_datetime_utc(file_stats.st_mtime),
            cls.obj_type_detector(path),
            uri,
            hash_id
        )
