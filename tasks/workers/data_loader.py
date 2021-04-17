from pathlib import Path
import shutil

from cumulusci.tasks.bulkdata.load import LoadData

from . import parallel_worker


class DataLoader(parallel_worker.ParallelWorker):
    def __init__(
        self,
        working_dir: Path,
        next_dir: Path,
    ):
        """Executed in parent process/thread"""
        self.working_dir = working_dir = Path(working_dir)
        mapping_file = working_dir / "temp_mapping.yml"
        database_file = working_dir / "generated_data.db"
        assert mapping_file.exists(), mapping_file
        assert database_file.exists(), database_file
        database_url = f"sqlite:///{database_file}"

        self.options = {
            "mapping": mapping_file,
            "reset_oids": False,
            "database_url": database_url,
        }
        self.next_dir = next_dir
        assert next_dir.exists(), next_dir
