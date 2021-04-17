from pathlib import Path
import shutil

from cumulusci.tasks.bulkdata.generate_from_yaml import GenerateDataFromYaml

from . import parallel_worker


class DataGenerator(parallel_worker.ParallelWorker):
    def __init__(self, working_parent_dir, batch_size, template_path, recipe: Path, num_records_tablename: str, idx: int, next_dir:Path):
        """Executed in parent process/thread"""
        self.working_dir = working_parent_dir / (str(idx) + "_" + str(batch_size))
        shutil.copytree(template_path, self.working_dir)
        assert self.working_dir.exists()
        self.database_file = self.working_dir / "generated_data.db"
        assert self.database_file.exists()
        self.batch_size = batch_size
        assert recipe.exists()
        assert isinstance(batch_size, int)
        assert isinstance(num_records_tablename, str)
        self.num_records_tablename = num_records_tablename
        mapping_file = self.working_dir / "temp_mapping.yml"
        assert mapping_file.exists()
        self.recipe = recipe
        self.next_dir = next_dir
        assert self.next_dir.exists()

    def run(self):
        database_url = f"sqlite:///{self.database_file}"
        options = {
            "generator_yaml": str(self.recipe),
            "database_url": database_url,
            "working_directory": self.working_dir,
            "num_records": self.batch_size,
            "reset_oids": False,
            "continuation_file": f"{self.working_dir}/continuation.yml",
            "num_records_tablename": self.num_records_tablename,
        }
        self.invoke_subtask(GenerateDataFromYaml, options, self.working_dir, False)
        shutil.move(str(self.working_dir), str(self.next_dir))
