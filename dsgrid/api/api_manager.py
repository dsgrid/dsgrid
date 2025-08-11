import logging
import threading
from datetime import datetime
from pathlib import Path

from dsgrid.exceptions import DSGValueNotStored
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.files import load_data
from .models import StoreModel, AsyncTaskModel, AsyncTaskStatus, AsyncTaskType


MAX_CONCURRENT_ASYNC_TASKS = 4

logger = logging.getLogger(__name__)


class ApiManager:
    """Manages API requests"""

    def __init__(
        self,
        home_dir: str | Path,
        registry_manager: RegistryManager,
        max_concurrent_async_tasks=MAX_CONCURRENT_ASYNC_TASKS,
    ):
        self._home_dir = Path(home_dir)
        self._store = Store.load(self._home_dir)
        self._lock = threading.RLock()
        self._max_concurrent_async_tasks = max_concurrent_async_tasks
        self._cached_projects = {}
        self._registry_mgr = registry_manager

    def can_start_new_async_task(self):
        self._lock.acquire()
        try:
            return len(self._store.data.outstanding_async_tasks) < self._max_concurrent_async_tasks
        finally:
            self._lock.release()

    def initialize_async_task(self, task_type: AsyncTaskType) -> int:
        self._lock.acquire()
        try:
            num_outstanding = len(self._store.data.outstanding_async_tasks)
            # TODO: implement queueing so that we don't return an error
            if num_outstanding > self._max_concurrent_async_tasks:
                msg = f"Too many async tasks are already running: {num_outstanding}"
                raise Exception(msg)
            async_task_id = self._get_next_async_task_id()
            task = AsyncTaskModel(
                async_task_id=async_task_id,
                task_type=task_type,
                status=AsyncTaskStatus.IN_PROGRESS,
                start_time=datetime.now(),
            )
            self._store.data.async_tasks[async_task_id] = task
            self._store.data.outstanding_async_tasks.add(async_task_id)
            self._store.persist()
        finally:
            self._lock.release()

        logger.info("Initialized async_task_id=%s", async_task_id)
        return async_task_id

    def clear_completed_async_tasks(self):
        self._lock.acquire()
        try:
            to_remove = [
                x.async_task_id
                for x in self._store.data.async_tasks
                if x.status == AsyncTaskStatus.COMPLETE
            ]
            for async_task_id in to_remove:
                self._store.data.async_tasks.pop(async_task_id)
            self._store.persist()
            logger.info("Cleared %d completed tasks", len(to_remove))
        finally:
            self._lock.release()

    def get_async_task_status(self, async_task_id):
        """Return the status of the async ID."""
        self._lock.acquire()
        try:
            return self._store.data.async_tasks[async_task_id]
        finally:
            self._lock.release()

    def complete_async_task(self, async_task_id, return_code: int, result=None):
        """Complete an asynchronous operation."""
        self._lock.acquire()
        try:
            task = self._store.data.async_tasks[async_task_id]
            task.status = AsyncTaskStatus.COMPLETE
            task.return_code = return_code
            task.completion_time = datetime.now()
            self._store.data.outstanding_async_tasks.remove(async_task_id)
            if result is not None:
                task.result = result
            self._store.persist()
        finally:
            self._lock.release()

        logger.info("Completed async_task_id=%s", async_task_id)

    def list_async_tasks(self, async_task_ids=None, status=None) -> list[AsyncTaskModel]:
        """Return async tasks.

        Parameters
        ----------
        async_task_ids : list | None
            IDs of tasks for which to return status. If not set, return all statuses.
        status : AsyncTaskStatus | None
            If set, filter tasks by this status.

        """
        self._lock.acquire()
        try:
            if async_task_ids is not None:
                diff = set(async_task_ids).difference(self._store.data.async_tasks.keys())
                if diff:
                    msg = f"async_task_ids={diff} are not stored"
                    raise DSGValueNotStored(msg)
            tasks = (
                self._store.data.async_tasks.keys() if async_task_ids is None else async_task_ids
            )
            return [
                self._store.data.async_tasks[x]
                for x in tasks
                if status is None or self._store.data.async_tasks[x].status == status
            ]
        finally:
            self._lock.release()

    def _get_next_async_task_id(self) -> int:
        self._lock.acquire()
        try:
            next_id = self._store.data.next_async_task_id
            self._store.data.next_async_task_id += 1
            self._store.persist()
        finally:
            self._lock.release()

        return next_id

    def get_project(self, project_id):
        """Load a Project and cache it for future calls.
        Loading is slow and the Project isn't being changed by this API.
        """
        self._lock.acquire()
        try:
            project = self._cached_projects.get(project_id)
            if project is not None:
                return project
            project = self._registry_mgr.project_manager.load_project(project_id)
            self._cached_projects[project_id] = project
            return project
        finally:
            self._lock.release()


class Store:
    STORE_FILENAME = "api_server_store.json"

    def __init__(self, store_file: Path, data: StoreModel):
        self._store_file = store_file
        self.data = data

    @classmethod
    def load(cls, path: Path):
        # TODO: use MongoDB or some other db
        store_file = path / cls.STORE_FILENAME
        if store_file.exists():
            logger.info("Load from existing store: %s", store_file)
            store_data = load_data(store_file)
            return cls(store_file, StoreModel(**store_data))
        logger.info("Create new store: %s", store_file)
        return cls(store_file, StoreModel())

    def persist(self):
        self._store_file.write_text(self.data.model_dump_json(indent=2))
