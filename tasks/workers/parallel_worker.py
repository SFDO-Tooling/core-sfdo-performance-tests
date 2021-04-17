import typing as T
from pathlib import Path
import logging
from contextlib import contextmanager
import shutil


from cumulusci.core.config import TaskConfig, BaseProjectConfig, OrgConfig


# weird things happen when I try to use a DataClass
class ParallelWorker:
    TaskClass: type
    project_config: BaseProjectConfig
    org_config: OrgConfig
    spawn_class: type
    task_options: T.Mapping[str, T.Any]
    working_dir: Path
    output_dir: "DelayedPath"       # noQA
    failures_dir: Path
    redirect_logging: bool
    process: object = None

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            assert k in self.__class__.__annotations__, (k, self.__class__.__annotations__)
            setattr(self, k, v)

        for k in self.__class__.__annotations__:
            assert hasattr(self, k), f"Did not specify {k}"

    def start(self):
        self.process = self.spawn_class(target=self.run)
        self.process.start()

    def is_alive(self):
        return self.process.is_alive()

    def join(self):
        return self.process.join()

    def run(self):
        subtask_config = TaskConfig({"options": self.task_options})
        with self.make_logger(self.working_dir / f"{self.TaskClass.__name__}.log", self.redirect_logging) as logger:
            subtask = self.TaskClass(
                project_config=self.project_config,
                task_config=subtask_config,
                org_config=self.org_config,
                logger=logger,
            )
            try:
                subtask()
                self.output_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.output_dir))                
                logger.info("SubTask Success!")
            except BaseException as e:
                logger.info(f"Failure detected : {e}")
                exception_file = self.working_dir / "exception.txt"
                exception_file.write_text(str(e))
                self.failures_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.failures_dir))
                raise

    @contextmanager
    def make_logger(self, filename: Path, redirect_logging):
        if not redirect_logging:
            yield None
        else:
            with filename.open("w") as f:
                logger = logging.Logger(self.TaskClass.__name__)

                formatter = logging.Formatter(fmt="%(asctime)s: %(message)s")
                handler = logging.StreamHandler(stream=f)
                handler.setLevel(logging.DEBUG)
                handler.setFormatter(formatter)
                logger.addHandler(handler)
                yield logger
