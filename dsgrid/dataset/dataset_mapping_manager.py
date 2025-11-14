import logging
from pathlib import Path
from typing import Self

from dsgrid.query.dataset_mapping_plan import (
    DatasetMappingPlan,
    MapOperation,
    MapOperationCheckpoint,
)
from dsgrid.spark.types import DataFrame
from dsgrid.utils.files import delete_if_exists
from dsgrid.utils.spark import read_dataframe, write_dataframe
from dsgrid.utils.scratch_dir_context import ScratchDirContext

logger = logging.getLogger(__name__)


class DatasetMappingManager:
    """Manages the mapping operations for a dataset."""

    def __init__(
        self,
        dataset_id: str,
        plan: DatasetMappingPlan,
        scratch_dir_context: ScratchDirContext,
        checkpoint: MapOperationCheckpoint | None = None,
    ):
        self._dataset_id = dataset_id
        self._plan = plan
        self._scratch_dir_context = scratch_dir_context
        self._checkpoint = checkpoint
        self._checkpoint_file: Path | None = None

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *args, **kwargs) -> None:
        # Don't cleanup if an exception occurred.
        self.cleanup()

    @property
    def plan(self) -> DatasetMappingPlan:
        """Return the mapping plan for the dataset."""
        return self._plan

    @property
    def scratch_dir_context(self) -> ScratchDirContext:
        """Return the scratch_dir_context."""
        return self._scratch_dir_context

    def try_read_checkpointed_table(self) -> DataFrame | None:
        """Read the checkpointed table for the dataset, if it exists."""
        if self._checkpoint is None:
            return None
        return read_dataframe(self._checkpoint.persisted_table_filename)

    def get_completed_mapping_operations(self) -> set[str]:
        """Return the names of completed mapping operations."""
        return (
            set() if self._checkpoint is None else set(self._checkpoint.completed_operation_names)
        )

    def has_completed_operation(self, op: MapOperation) -> bool:
        """Return True if the mapping operation has been completed."""
        return op.name in self.get_completed_mapping_operations()

    def persist_table(self, df: DataFrame, op: MapOperation) -> DataFrame:
        """Persist the intermediate table to the filesystem and return the persisted DataFrame."""
        persisted_file = self._scratch_dir_context.get_temp_filename(
            suffix=".parquet", add_tracked_path=False
        )
        write_dataframe(df, persisted_file)
        self.save_checkpoint(persisted_file, op)
        logger.info("Persisted mapping operation name=%s to %s", op.name, persisted_file)
        return read_dataframe(persisted_file)

    def save_checkpoint(self, persisted_table: Path, op: MapOperation) -> None:
        """Save a checkpoint after persisting an operation to the filesystem."""
        completed_operation_names: list[str] = []
        for mapping_op in self._plan.list_mapping_operations():
            completed_operation_names.append(mapping_op.name)
            if mapping_op.name == op.name:
                break

        checkpoint = MapOperationCheckpoint(
            dataset_id=self._dataset_id,
            completed_operation_names=completed_operation_names,
            persisted_table_filename=persisted_table,
            mapping_plan_hash=self._plan.compute_hash(),
        )
        checkpoint_filename = self._scratch_dir_context.get_temp_filename(
            suffix=".json", add_tracked_path=False
        )
        checkpoint.to_file(checkpoint_filename)
        if self._checkpoint_file is not None and not self._plan.keep_intermediate_files:
            assert self._checkpoint is not None, self._checkpoint
            logger.info("Remove previous checkpoint: %s", self._checkpoint_file)
            delete_if_exists(self._checkpoint_file)
            delete_if_exists(self._checkpoint.persisted_table_filename)

        self._checkpoint = checkpoint
        self._checkpoint_file = checkpoint_filename
        logger.info("Saved checkpoint in %s", self._checkpoint_file)

    def cleanup(self) -> None:
        """Cleanup the intermediate files. Call if the operation completed succesfully."""
        if self._plan.keep_intermediate_files:
            logger.info("Keeping intermediate files for dataset %s", self._dataset_id)
            return

        if self._checkpoint_file is not None:
            logger.info("Removing checkpoint filename %s", self._checkpoint_file)
            delete_if_exists(self._checkpoint_file)
            self._checkpoint_file = None
        if self._checkpoint is not None:
            logger.info(
                "Removing persisted intermediate table filename %s",
                self._checkpoint.persisted_table_filename,
            )
            delete_if_exists(self._checkpoint.persisted_table_filename)
            self._checkpoint = None
