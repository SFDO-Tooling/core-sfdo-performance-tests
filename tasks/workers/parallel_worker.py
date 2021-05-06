import typing as T
from pathlib import Path
import logging
from contextlib import contextmanager
import shutil
import json
from traceback import format_exc
from multiprocessing import get_context
from threading import Thread
from tempfile import gettempdir

from cumulusci.core.exceptions import ServiceNotConfigured
from cumulusci.core.config import TaskConfig
from cumulusci.core.utils import import_global

from cumulusci.core.config import (
    UniversalConfig,
    ServiceConfig,
    BaseProjectConfig,
    OrgConfig,
)


logger = logging.getLogger(__file__)
logger.setLevel(logging.DEBUG)


def get_annotations(cls: type):
    "https://stackoverflow.com/questions/64309238/is-there-built-in-method-to-get-all-annotations-from-all-base-classes-in-pyt"
    all_ann = [c.__annotations__ for c in cls.mro()[:-1]]
    all_ann_names = set()
    for aa in all_ann[::-1]:
        all_ann_names.update(aa.keys())
    return all_ann_names


class SharedConfig:
    task_class: type
    task_options: T.Mapping[str, T.Any]
    project_config: BaseProjectConfig
    org_config: OrgConfig
    failures_dir: Path
    redirect_logging: bool
    connected_app: ServiceConfig  # can this be computed

    def __init__(self, validate: bool = False, **kwargs):
        valid_property_names = get_annotations(self.__class__)
        for k, v in kwargs.items():
            if validate and k not in valid_property_names:
                raise AssertionError(
                    f"Unknown property `{k}`. Should be one of {valid_property_names}"
                )
            setattr(self, k, v)
        for k in self.__class__.__annotations__:
            assert hasattr(self, k), f"Did not specify {k}"


class WorkerQueueConfig(SharedConfig):
    name: str
    parent_dir: Path
    failures_dir: Path = None
    task_class: T.Callable
    queue_size: int
    num_workers: int
    spawn_class: T.Callable
    outbox_dir: Path

    def __init__(self, **kwargs):
        kwargs.setdefault("failures_dir", kwargs["parent_dir"] / "failures")
        super().__init__(True, **kwargs)


# TODO: investigate dataclass-json
class WorkerConfig(SharedConfig):
    connected_app: ServiceConfig
    working_dir: Path

    def as_dict(self):
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
    def from_dict(worker_config_json):
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
            redirect_logging=worker_config_json["redirect_logging"],
        )


def dotted_class_name(cls):
    return cls.__module__ + "." + cls.__name__


class TaskWorker:
    def __init__(self, worker_dict):
        self.worker_config = WorkerConfig.from_dict(worker_dict)
        self.redirect_logging = worker_dict["redirect_logging"]

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


def run_task_in_worker(worker_dict):
    worker = TaskWorker(worker_dict)
    return worker.run()


def simplify(x):
    print(x)
    if isinstance(x, Path):
        return str(x)
    if hasattr(x, "isoformat"):
        return x.isoformat()
    print("Unknown type", type(x))  # FIXME


class ParallelWorker:
    context = get_context("spawn")
    Process = context.Process
    Thread = Thread

    def __init__(self, spawn_class, worker_config: WorkerConfig):
        self.spawn_class = spawn_class
        self.worker_config = worker_config

    def _validate_worker_config_is_simple(self, worker_config):
        assert json.dumps(worker_config, default=simplify)

    def start(self):
        dct = self.worker_config.as_dict()
        self._validate_worker_config_is_simple(dct)

        self.process = self.spawn_class(target=run_task_in_worker, args=[dct])
        self.process.start()

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


class WorkerQueue:
    def __init__(
        self,
        queue_config: WorkerQueueConfig,
    ):
        self.config = queue_config
        # convenience access to names
        self.__dict__.update(queue_config.__dict__)

        self.inbox_dir = self.parent_dir / f"{self.name}_inbox"
        self.inbox_dir.mkdir()
        self.in_progress_dir = self.parent_dir / f"{self.name}_inprogress"
        self.in_progress_dir.mkdir()

        self.outbox_dir = self.parent_dir / f"{self.name}_outbox"
        self.outbox_dir.mkdir()

        self.workers = []

    @property
    def full(self):
        return len(tuple(self.inbox_dir.iterdir())) >= self.queue_size

    @property
    def free_workers(self) -> int:
        return self.config.num_workers - len(self.workers)

    def push(
        self,
        job_dir: T.Optional[Path],
        worker_config: WorkerConfig,
        name: T.Optional[str],
    ):
        assert not (job_dir and name), "Supply name or job_dir, not both"
        assert job_dir or name, "Supply name or job_dir"

        if self.full:
            raise ValueError("Queue is full")

        if not job_dir:
            job_dir = Path(gettempdir()) / name
            job_dir.mkdir()

        self._queue_job(job_dir, worker_config)
        self.tick()

    @property
    def queued_jobs(self):
        return list(self.inbox_dir.iterdir())

    def _start_job(self, job_dir: Path):
        options = self._get_job_options(job_dir)
        self.in_progress_dir.mkdir(exist_ok=True)

        working_dir = Path(shutil.move(job_dir, self.in_progress_dir))

        worker_config = WorkerConfig(
            **self.config.__dict__, working_dir=working_dir, output_dir=self.outbox_dir
        )
        worker_config.task_options.update(options)
        worker = ParallelWorker(self.config.spawn_class, worker_config)
        worker.start()
        self.workers.append(worker)

    def _get_job_options(self, job_dir: Path):
        job_dir_options = job_dir / "options.json"
        rc = {}
        if job_dir_options.exists():
            rc = json.loads(job_dir_options.read_text())
            job_dir_options.unlink()

        return rc

    def _queue_job(self, job_dir: Path, options: dict):
        job_dir = shutil.move(job_dir, self.inbox_dir)
        job_dir_options = Path(job_dir, "options.json")
        job_dir_options.write_text(json.dumps(options))

    def tick(self):
        live_workers = []
        dead_workers = []
        for worker in self.workers:
            if worker.is_alive():
                live_workers.append(worker)
            else:
                dead_workers.append(worker)

        self.workers = live_workers

        for idx, job_dir in zip(range(self.free_workers), self.queued_jobs):
            logger.info(f"Starting job {job_dir}")
            self._start_job(job_dir)


import pytest
from cumulusci.tasks.util import Sleep


class TestWorkerQueue:
    def test_worker_queue(self):
        from tempfile import TemporaryDirectory

        with TemporaryDirectory() as t:

            class DelaySpawner:
                def __init__(self, target, args):
                    logger.info(f"Creating spawner {target} {args}")
                    self.target = target
                    self.args = args
                    self._is_alive = False

                def start(self):
                    logger.info(f"Starting spawner {self}")
                    self._is_alive = True

                def _finish(self):
                    self.target(*self.args)
                    self._is_alive = False

                def is_alive(self):
                    logger.info(f"Checking alive: {self}: {self._is_alive}")
                    return self._is_alive

            t = Path(t)

            work = t / "start"
            work.mkdir()

            outbox = t / "outbox"
            outbox.mkdir()

            project_config = BaseProjectConfig(
                UniversalConfig(),
                config={"project": {"package": "packageA"}},
            )
            org_config = OrgConfig({}, "dummy_config")

            config = WorkerQueueConfig(
                name="start",
                task_class=Sleep,
                task_options={"seconds": 0},
                project_config=project_config,
                org_config=org_config,
                connected_app=None,
                redirect_logging=True,
                parent_dir=t,
                queue_size=3,
                num_workers=2,
                spawn_class=DelaySpawner,
                outbox_dir=outbox,
            )

            q = WorkerQueue(config)
            assert not q.full
            assert q.free_workers == 2
            assert q.queued_jobs == []
            q.push(None, {}, "a")
            assert not q.full
            assert q.free_workers == 1
            assert len(q.queued_jobs) == 0
            q.tick()
            assert not q.full
            assert q.free_workers == 1
            assert len(q.queued_jobs) == 0
            q.workers[0].process._finish()
            q.tick()
            assert not q.full
            assert q.free_workers == 2
            assert len(q.queued_jobs) == 0
            q.tick()
            assert not q.full
            assert q.free_workers == 2
            assert len(q.queued_jobs) == 0
            q.push(None, {}, "bb")
            q.tick()
            assert not q.full
            assert q.free_workers == 1
            assert len(q.queued_jobs) == 0
            q.push(None, {}, "cc")
            q.tick()
            assert not q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 0
            q.push(None, {}, "dd")
            q.tick()
            assert not q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 1
            q.push(None, {}, "ee")
            q.tick()
            assert not q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 2
            q.push(None, {}, "ff")
            q.tick()
            assert q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 3

            with pytest.raises(ValueError):
                q.push(None, {}, "hh")

            assert q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 3

            for worker in q.workers:
                worker.process._finish()

            assert q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 3

            q.tick()

            assert not q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 1

            for worker in q.workers:
                worker.process._finish()

            q.tick()

            assert not q.full
            assert q.free_workers == 1
            assert len(q.queued_jobs) == 0

            for worker in q.workers:
                worker.process._finish()

            q.tick()

            assert not q.full
            assert q.free_workers == 2
            assert len(q.queued_jobs) == 0

            q.push(None, {}, "ii")
            q.push(None, {}, "jj")
            q.push(None, {}, "kk")
            q.push(None, {}, "ll")
            q.push(None, {}, "mm")
            with pytest.raises(ValueError):
                q.push(None, {}, "nn")

            q.tick()

            assert q.full
            assert q.free_workers == 0
            assert len(q.queued_jobs) == 3
