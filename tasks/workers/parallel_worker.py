import typing as T
from pathlib import Path
import logging
from contextlib import contextmanager
import shutil
import json
from traceback import format_exc

from cumulusci.core.exceptions import ServiceNotConfigured
from cumulusci.core.config import TaskConfig
from cumulusci.core.utils import import_global
from cumulusci.core.config import (
    UniversalConfig,
    ServiceConfig,
    BaseProjectConfig,
    OrgConfig,
)


# TODO: investigate dataclass-json
class WorkerConfig:
    task_class: type
    task_options: T.Mapping[str, T.Any]
    project_config: BaseProjectConfig
    org_config: OrgConfig
    working_dir: Path
    output_dir: Path
    failures_dir: Path
    connected_app: ServiceConfig
    redirect_logging: bool

    def __init__(self, **kwargs):
        for k, v in kwargs.items():
            assert k in self.__class__.__annotations__, (
                k,
                self.__class__.__annotations__,
            )
            setattr(self, k, v)
        for k in self.__class__.__annotations__:
            assert hasattr(self, k), f"Did not specify {k}"

    def as_json(self):
        return {
            "task_class": dotted_class_name(self.task_class),
            "org_config_class": dotted_class_name(self.org_config.__class__),
            "task_options": self.task_options,
            "working_dir": str(self.working_dir),
            "output_dir": str(self.output_dir),
            "failures_dir": str(self.failures_dir),
            "org_config": (
                self.org_config.config,
                self.org_config.name,
            ),
            "connected_app": self.connected_app.config if self.connected_app else None,
            "redirect_logging": self.redirect_logging,
            "project_config": {
                "project": {"package": self.project_config.config["project"]["package"]}
            },
        }

    @staticmethod
    def from_json(worker_config_json):
        org_config_class = import_global(worker_config_json["org_config_class"])
        org_config = org_config_class(*worker_config_json["org_config"])

        task_options = worker_config_json["task_options"]

        universal_config = UniversalConfig()
        project_config = BaseProjectConfig(
            universal_config,
            config=worker_config_json["project_config"],
        )
        return WorkerConfig(
            task_class=import_global(worker_config_json["task_class"]),
            task_options=task_options,
            project_config=project_config,
            org_config=org_config,
            working_dir=Path(worker_config_json["working_dir"]),
            output_dir=Path(worker_config_json["output_dir"]),
            failures_dir=Path(worker_config_json["failures_dir"]),
            connected_app=ServiceConfig(worker_config_json["connected_app"])
            if worker_config_json["connected_app"]
            else None,
            redirect_logging=worker_config_json["connected_app"],
        )


def dotted_class_name(cls):
    return cls.__module__ + "." + cls.__name__


class TaskWorker:
    def __init__(self, worker_config_json):
        self.worker_config = WorkerConfig.from_json(worker_config_json)
        self.redirect_logging = worker_config_json["redirect_logging"]

    def __getattr__(self, name):
        return getattr(self.worker_config, name)

    def _make_task(self, task_class, logger):
        task_config = TaskConfig({"options": self.task_options})
        connected_app = self.connected_app
        keychain = SubprocessKeyChain(connected_app)
        self.project_config.set_keychain(keychain)
        self.org_config.keychain = keychain
        return task_class(
            project_config=self.project_config,
            task_config=task_config,
            org_config=self.org_config,
            logger=logger,
        )

    def save_exception(self, e):
        exception_file = self.working_dir / "exception.txt"
        exception_file.write_text(format_exc())

    def run(self):
        with self.make_logger() as logger:
            try:
                self.subtask = self._make_task(self.task_class, logger)
                self.subtask()
            except BaseException as e:
                logger.info(f"Failure detected: {e}")
                self.save_exception(e)
                self.failures_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.failures_dir))
                raise

            try:
                self.output_dir.mkdir(exist_ok=True)
                shutil.move(str(self.working_dir), str(self.output_dir))
                logger.info("SubTask Success!")
            except BaseException as e:
                logger.info(f"Failure detected: {e}")
                self.save_exception(e)
                raise

    @contextmanager
    def make_logger(self):
        filename = self.working_dir / f"{self.task_class.__name__}.log"
        with filename.open("w") as f:
            logger = logging.Logger(self.task_class.__name__)

            formatter = logging.Formatter(fmt="%(asctime)s: %(message)s")
            handler = logging.StreamHandler(stream=f)
            handler.setLevel(logging.DEBUG)
            handler.setFormatter(formatter)
            logger.addHandler(handler)
            logger.propagate = False
            yield logger


def run_task_in_worker(worker_json):
    worker = TaskWorker(json.loads(worker_json))
    return worker.run()


def simplify(x):
    if isinstance(x, Path):
        return str(x)
    if hasattr(x, "isoformat"):
        return x.isoformat()
    print("Unknown type", type(x))  # FIXME


class ParallelWorker:
    def __init__(self, spawn_class, **kwargs):
        self.spawn_class = spawn_class
        self.worker_config = json.dumps(
            WorkerConfig(**kwargs).as_json(), default=simplify
        )

    def start(self):
        self.process = self.spawn_class(
            target=run_task_in_worker, args=[self.worker_config]
        )
        self.process.start()
        # run_task_in_worker(self.worker_config)

    def is_alive(self):
        return self.process.is_alive()

    def join(self):
        return self.process.join()


class SubprocessKeyChain(T.NamedTuple):
    connected_app: T.Any = None

    def get_service(self, name):
        if name == "connected_app" and self.connected_app:
            return self.connected_app

        raise ServiceNotConfigured(name)

    def set_org(self, *args):
        pass
