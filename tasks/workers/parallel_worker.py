import typing as T
from pathlib import Path
import logging
from contextlib import contextmanager
import shutil
from cumulusci.core.exceptions import ServiceNotConfigured


from cumulusci.core.config import TaskConfig


# weird things happen when I try to use a DataClass
class ParallelWorker:
    TaskClass: type
    spawn_class: type
    task_options: T.Mapping[str, T.Any]
    working_dir: Path
    output_dir: "DelayedPath"  # noQA
    failures_dir: Path
    redirect_logging: bool
    process: object = None

    def __init__(self, project_config, org_config, **kwargs):
        for k, v in kwargs.items():
            assert k in self.__class__.__annotations__, (
                k,
                self.__class__.__annotations__,
            )
            setattr(self, k, v)

        for k in self.__class__.__annotations__:
            assert hasattr(self, k), f"Did not specify {k}"
        self.subtask = self._init_task(project_config, org_config)

    def _init_task(self, parent_project_config, org_config):
        project_config = parent_project_config.construct_subproject_config(repo_info={"root": parent_project_config.repo_root})
        project_config.set_keychain(
            self._create_subprocess_keychain(parent_project_config)
        )

        subtask_config = TaskConfig({"options": self.task_options})

        subtask = self.TaskClass(
            project_config=project_config,
            task_config=subtask_config,
            org_config=org_config,
            logger=None,
        )

        return subtask

    def _create_subprocess_keychain(self, project_config):
        try:
            connected_app = project_config.keychain.get_service("connected_app")
        except ServiceNotConfigured:
            connected_app = None

        return SubprocessKeyChain(connected_app)

    def start(self):
        self.process = self.spawn_class(target=self.run)
        self.process.start()

    def is_alive(self):
        return self.process.is_alive()

    def join(self):
        return self.process.join()

    def run(self):
        with self.make_logger(
            self.working_dir / f"{self.TaskClass.__name__}.log", self.redirect_logging
        ) as logger:
            self.subtask.logger = logger
            try:
                self.subtask()
            except BaseException as e:
                logger.info(f"Failure detected: {e}")
                exception_file = self.working_dir / "exception.txt"
                exception_file.write_text(str(e))
                self.failures_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.failures_dir))
                raise

            try:
                self.output_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.output_dir))
                logger.info("SubTask Success!")
            except BaseException:
                # TODO: Think more about this
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
                logger.propagate = False
                yield logger


class SubprocessKeyChain(T.NamedTuple):
    connected_app: T.Any

    def get_service(self, name):
        if name == "connected_app":
            return self.connected_app

        raise ServiceNotConfigured(name)
