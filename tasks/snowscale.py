import shutil
import time
import typing as T

from pathlib import Path
from multiprocessing import Process, Value
from tempfile import mkdtemp
from contextlib import contextmanager, redirect_stdout, redirect_stderr
from dataclasses import dataclass


from sqlalchemy import MetaData, create_engine

from cumulusci.tasks.bulkdata.generate_from_yaml import GenerateDataFromYaml
from cumulusci.tasks.bulkdata.load import LoadData
from cumulusci.tasks.salesforce import BaseSalesforceApiTask
from cumulusci.tasks.bulkdata.generate_and_load_data_from_yaml import (
    GenerateAndLoadDataFromYaml,
)
from cumulusci.core.config import TaskConfig
from cumulusci.cli.logger import init_logger

bulkgen_task = "cumulusci.tasks.bulkdata.generate_from_yaml.GenerateDataFromYaml"


@dataclass
class UploadStatus:
    confirmed_count_in_org: int
    rows_being_generated: int
    rows_queued: int
    rows_being_loaded: int
    rows_finished: int
    target_count: int
    base_batch_size: int
    delay_multiple: int
    upload_queue_backlog: int
    user_max_num_uploader_workers: int
    user_max_num_generator_workers: int

    @property
    def max_needed_generators_to_fill_queue(self):
        return max(self.user_max_num_uploader_workers - self.upload_queue_backlog, 0)

    @property
    def max_needed_generators_to_fill_org(self):
        if self.done:
            return 0
        else:
            return max(self.min_rows_remaining // self.batch_size, 1)

    @property
    def total_needed_generators(self):
        if self.wait_for_org_to_catch_up:
            return 0
        return min(
            self.user_max_num_generator_workers,
            self.max_needed_generators_to_fill_org,
            self.max_needed_generators_to_fill_queue,
        )

    @property
    def wait_for_org_to_catch_up(self):
        return self.maximum_estimated_count_so_far > self.target_count

    @property
    def total_in_flight(self):
        return self.rows_being_generated + self.rows_queued + self.rows_being_loaded

    @property
    def maximum_estimated_count_so_far(self):
        return self.confirmed_count_in_org + self.total_in_flight

    @property
    def min_rows_remaining(self):
        return max(0, self.target_count - self.maximum_estimated_count_so_far)

    @property
    def throttling(self):
        return self.min_rows_remaining < 1

    @property
    def batch_size(self):
        if self.min_rows_remaining > 0:
            return self.base_batch_size * self.delay_multiple
        else:
            return self.base_batch_size

    @property
    def done(self):
        return self.confirmed_count_in_org >= self.target_count

    def _display(self, detailed=False):
        if detailed:
            keys = dir(self)
        else:
            keys = [
                "target_count",
                "confirmed_count_in_org",
                "batch_size",
                "rows_being_generated",
                "rows_queued",
                "rows_being_loaded",
                "rows_finished",
                "wait_for_org_to_catch_up",
            ]
        return "\n" + "\n".join(f"{a}: {getattr(self, a)}" for a in keys if not a[0] == "_")


class Snowfakery(BaseSalesforceApiTask):
    """ """

    task_docs = """
    """
    task_options = {
        "recipe": {"required": True},
        "num_generator_workers": {},
        "num_uploader_workers": {},
        "loading_rules": {},  # TODO: Impl, Docs
        "working_directory": {},  # TODO: Impl, Docs
        "recipe_options": {},  # TODO: Impl, Docs
        "num_records": {},  # TODO: Docs
        "num_records_tablename": {},  # TODO : Better name
        "max_batch_size": {},  # TODO Impl, Docs
        "unified_logging": {},  # TODO RETHINK
    }

    def _init_options(self, kwargs):
        args = {"data_generation_task": bulkgen_task, **kwargs}

        super()._init_options(args)

    def _validate_options(self):
        super()._validate_options()
        # long-term solution: psutil.cpu_count(logical=False)
        self.num_generator_workers = int(self.options.get("num_generator_workers", 4))
        self.num_uploader_workers = int(self.options.get("num_uploader_workers", 15))
        # self.num_uploader_workers = 3  # FIXME
        # Do not store recipe due to MetaDeploy options freezing
        recipe = Path(self.options.get("recipe"))
        assert recipe.exists()
        assert isinstance(self.options.get("num_records_tablename"), str)
        assert int(self.options.get("num_records")), self.options.get("num_records")
        self.unified_logging = self.options.get("unified_logging")

    def _run_task(self):
        self._done = Value("i", False)
        self.max_batch_size = self.options.get("max_batch_size", 250_000)
        self.recipe = Path(self.options.get("recipe"))
        self.job_counter = 1
        self.delay_multiple = 1

        working_directory = self.options.get("working_directory")
        if working_directory:
            working_directory = Path(working_directory)
        with self._generate_and_load_initial_batch(working_directory) as (
            tempdir,
            template_path,
        ):
            self.logger.info(f"Working directory is {tempdir}")
            # os.system(f"code {tempdir}")  # FIXME
            assert tempdir.exists()
            self.generators_path = Path(tempdir) / "1_generators"
            self.generators_path.mkdir()
            self.queue_for_loading_directory = Path(tempdir) / "2_load_queue"
            self.queue_for_loading_directory.mkdir()
            self.loaders_path = Path(tempdir) / "3_loaders"
            self.loaders_path.mkdir()
            self.finished_directory = Path(tempdir, "4_finished")
            self.finished_directory.mkdir()

            self._loop(template_path, tempdir)

            self._done.value = True

            for char in "☃  D ❄ O ❆ N ❉ E ☃":
                print(char, end="", flush=True)
                time.sleep(0.10)
            print()

    def _loop(self, template_path, tempdir):
        self._parallelized_loop(template_path, tempdir)

    def _parallelized_loop(self, template_path, tempdir):
        generator_workers = []
        upload_workers = []

        upload_status = self.generate_upload_status()

        # TODO: how can I ensure I'm making forward progress
        while not upload_status.done:
            self.logger.info(
                "\n********** PROGRESS *********",
            )
            self.logger.info(upload_status._display(detailed=True))
            upload_workers = self._spawn_transient_upload_workers(upload_workers)
            generator_workers = [
                worker for worker in generator_workers if worker.is_alive()
            ]

            if upload_status.max_needed_generators_to_fill_queue == 0:
                self.logger.info("WAITING FOR UPLOAD QUEUE TO CATCH UP")
                self.delay_multiple *= 2
                self.logger.info(f"Batch size multiple={self.delay_multiple}")
            else:
                generator_workers = self._spawn_transient_generator_workers(
                    generator_workers, upload_status, template_path
                )
            self.logger.info(f"Generator Workers: {len(generator_workers)}")
            self.logger.info(f"Upload Workers: {len(upload_workers)}")
            self.logger.info(f"Queue size: {upload_status.upload_queue_backlog}")
            self.logger.info(f"Working Directory: {tempdir}")
            time.sleep(3)
            upload_status = self.generate_upload_status()

        for worker in generator_workers:
            worker.join()
        for worker in upload_workers:
            worker.join()

        self.logger.info(self.generate_upload_status()._display())

    def _spawn_transient_upload_workers(self, upload_workers):
        upload_workers = [worker for worker in upload_workers if worker.is_alive()]
        current_upload_workers = len(upload_workers)
        if current_upload_workers < self.num_uploader_workers:
            free_workers = self.num_uploader_workers - current_upload_workers
            jobs_to_be_done = list(self.queue_for_loading_directory.glob("*_*"))
            jobs_to_be_done.sort(key=lambda j: int(j.name.split("_")[0]))

            jobs_to_be_done = jobs_to_be_done[0:free_workers]
            for job in jobs_to_be_done:
                working_directory = shutil.move(str(job), str(self.loaders_path))
                process = Process(target=self._load_process, args=[working_directory])
                # add an error trapping/reporting wrapper
                process.start()
                upload_workers.append(process)
        return upload_workers

    def _spawn_transient_generator_workers(self, workers, upload_status, template_path):
        workers = [worker for worker in workers if worker.is_alive()]
        # TODO: Check for errors!!!

        total_needed_workers = upload_status.total_needed_generators
        new_workers = total_needed_workers - len(workers)

        for idx in range(new_workers):
            self.job_counter += 1

            args = [
                self.generators_path,
                upload_status.batch_size,
                template_path,
                self.job_counter,
            ]
            process = Process(target=self._do_generate, args=args)
            # add an error trapping/reporting wrapper
            process.start()

            workers.append(process)
        return workers

    def _do_generate(self, working_parent_dir, batch_size, template_path, idx: int):
        working_dir = working_parent_dir / (str(idx) + "_" + str(batch_size))
        shutil.copytree(template_path, working_dir)
        database_file = working_dir / "generated_data.db"
        # not needed once just_once is implemented
        mapping_file = working_dir / "temp_mapping.yml"
        database_url = f"sqlite:///{database_file}"
        #
        assert working_dir.exists()
        options = {
            "generator_yaml": str(self.recipe),
            "database_url": database_url,
            "working_directory": working_dir,
            "num_records": batch_size,
            "generate_mapping_file": mapping_file,
            "reset_oids": False,
            "continuation_file": f"{working_dir}/continuation.yml",
            "num_records_tablename": self.options.get("num_records_tablename"),
        }
        self._invoke_subtask(GenerateDataFromYaml, options, working_dir)
        assert mapping_file.exists()
        shutil.move(str(working_dir), str(self.queue_for_loading_directory))

    def _load_process(self, working_directory: str):
        working_directory = Path(working_directory)
        mapping_file = working_directory / "temp_mapping.yml"
        database_file = working_directory / "generated_data.db"
        assert mapping_file.exists(), mapping_file
        assert database_file.exists(), database_file
        database_url = f"sqlite:///{database_file}"

        options = {
            "mapping": mapping_file,
            "reset_oids": False,
            "database_url": database_url,
        }
        self._invoke_subtask(LoadData, options, working_directory)
        shutil.move(str(working_directory), str(self.finished_directory))

    def _invoke_subtask(
        self, TaskClass: type, subtask_options: T.Mapping[str, T.Any], working_dir: Path
    ):
        subtask_config = TaskConfig({"options": subtask_options})
        subtask = TaskClass(
            project_config=self.project_config,
            task_config=subtask_config,
            org_config=self.org_config,
            flow=self.flow,
            name=self.name,
            stepnum=self.stepnum,
        )
        with self._add_tempfile_logger(working_dir / f"{TaskClass.__name__}.log"):
            try:
                subtask()
            except Exception as e:
                self.logger.warn("Exception Raised! {} {}", TaskClass.__name__, e)
                self._done.value = True
                raise e

    # TODO: Probably don't need this
    def done(self):
        if self._done.value:
            return True

    def rows_in_dir(self, dir):
        idx_and_counts = (subdir.name.split("_") for subdir in dir.glob("*_*"))
        return sum(int(count) for (idx, count) in idx_and_counts)

    def generate_upload_status(self):
        return UploadStatus(
            confirmed_count_in_org=self.get_org_record_count(),
            target_count=int(self.options.get("num_records")),
            rows_being_generated=self.rows_in_dir(self.generators_path),
            rows_queued=self.rows_in_dir(self.queue_for_loading_directory),
            delay_multiple=self.delay_multiple,
            # note that these may count as already imported in the org
            rows_being_loaded=self.rows_in_dir(self.loaders_path),
            upload_queue_backlog=sum(
                1 for dir in self.queue_for_loading_directory.glob("*_*")
            ),
            rows_finished=self.rows_in_dir(self.finished_directory),
            base_batch_size=500,  # FIXME
            user_max_num_uploader_workers=self.num_uploader_workers,
            user_max_num_generator_workers=self.num_generator_workers,
        )

    def get_org_record_count(self):
        sobject = self.options.get("num_records_tablename")
        query = f"select count(Id) from {sobject}"
        count = self.sf.query(query)["records"][0]["expr0"]
        return int(count)
        # I'll probably need this code when I hit big orgs

        # data = self.sf.restful(f"limits/recordCount?sObjects={table}")
        # count = int(data["sObjects"][0]["count"])

    @contextmanager
    def workingdir_or_tempdir(self, working_directory: T.Optional[Path]):
        if working_directory:
            working_directory.mkdir()
            self.logger.info(f"Working Directory {working_directory}")
            yield working_directory
        else:
            # with TemporaryDirectory() as tempdir:
            # yield tempdir

            # do not clean up tempdirs for now
            tempdir = mkdtemp()
            self.logger.info(f"Working Directory {tempdir}")
            yield tempdir

    @contextmanager
    def _generate_and_load_initial_batch(
        self, working_directory: T.Optional[Path]
    ) -> Path:
        with self.workingdir_or_tempdir(working_directory) as tempdir:
            template_dir = Path(tempdir) / "template"
            template_dir.mkdir()
            self._generate_and_load_batch(
                template_dir, {"generator_yaml": self.options.get("recipe")}
            )

            yield Path(tempdir), template_dir

    def _generate_and_load_batch(self, tempdir, options) -> Path:
        options = {**options, "working_directory": tempdir}
        self._invoke_subtask(GenerateAndLoadDataFromYaml, options, tempdir)
        generated_data = tempdir / "generated_data.db"
        assert generated_data.exists(), generated_data
        database_url = f"sqlite:///{generated_data}"
        self._cleanup_object_tables(*self._setup_engine(database_url))

    @contextmanager
    def _add_tempfile_logger(self, my_log: Path):
        if self.unified_logging:
            i = init_logger()       # FIXME: this is messy
            if hasattr(i, "__enter__"):
                i.__enter__()
            yield
        else:
            with open(my_log, "w") as f:
                with redirect_stdout(f), redirect_stderr(f), init_logger():
                    yield f
                # handler = logging.StreamHandler(stream=f)
                # handler.setLevel(logging.DEBUG)
                # formatter = coloredlogs.ColoredFormatter(fmt="%(asctime)s: %(message)s")
                # handler.setFormatter(formatter)

                # rootLogger.addHandler(handler)
                # try:
                #     yield f
                # finally:
                #     rootLogger.removeHandler(handler)

    def _setup_engine(self, database_url):
        """Set up the database engine"""
        engine = create_engine(database_url)

        metadata = MetaData(engine)
        metadata.reflect()
        return engine, metadata

    def _cleanup_object_tables(self, engine, metadata):
        """Delete all tables that do not relate to id->OID mapping"""
        tables = metadata.tables
        tables_to_drop = [
            table
            for tablename, table in tables.items()
            if not tablename.endswith("sf_ids")
        ]
        if tables_to_drop:
            metadata.drop_all(tables=tables_to_drop)


def test():
    u = UploadStatus(
        base_batch_size=5000,
        confirmed_count_in_org=20000,
        delay_multiple=1,
        rows_being_generated=5000,
        rows_being_loaded=20000,
        rows_queued=0,
        target_count=30000,
        upload_queue_backlog=1,
        user_max_num_generator_workers=4,
        user_max_num_uploader_workers=15,
    )
    assert u.total_needed_generators == 1, u.total_needed_generators

    u = UploadStatus(
        base_batch_size=5000,
        confirmed_count_in_org=0,
        delay_multiple=1,
        rows_being_generated=5000,
        rows_being_loaded=20000,
        rows_queued=0,
        target_count=30000,
        upload_queue_backlog=1,
        user_max_num_generator_workers=4,
        user_max_num_uploader_workers=15,
    )
    assert u.total_needed_generators == 1, u.total_needed_generators

    u = UploadStatus(
        base_batch_size=5000,
        confirmed_count_in_org=0,
        delay_multiple=1,
        rows_being_generated=5000,
        rows_being_loaded=15000,
        rows_queued=0,
        target_count=30000,
        upload_queue_backlog=1,
        user_max_num_generator_workers=4,
        user_max_num_uploader_workers=15,
    )
    assert u.total_needed_generators == 2, u.total_needed_generators

    u = UploadStatus(
        base_batch_size=5000,
        confirmed_count_in_org=29000,
        delay_multiple=1,
        rows_being_generated=0,
        rows_being_loaded=0,
        rows_queued=0,
        target_count=30000,
        upload_queue_backlog=0,
        user_max_num_generator_workers=4,
        user_max_num_uploader_workers=15,
    )
    assert u.total_needed_generators == 1, u.total_needed_generators

    u = UploadStatus(
        base_batch_size=5000,
        confirmed_count_in_org=4603,
        delay_multiple=1,
        rows_being_generated=5000,
        rows_being_loaded=20000,
        rows_queued=0,
        target_count=30000,
        upload_queue_backlog=0,
        user_max_num_generator_workers=4,
        user_max_num_uploader_workers=15,
    )
    assert u.total_needed_generators == 1, u.total_needed_generators


if __name__ == "__main__":
    test()