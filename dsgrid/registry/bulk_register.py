import getpass
import logging
import shutil
from pathlib import Path
from uuid import uuid4

from dsgrid.config.registration_models import RegistrationModel, RegistrationJournal
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.id_remappings import (
    map_dimension_ids_to_names,
    map_dimension_names_to_ids,
    map_dimension_mapping_names_to_ids,
    replace_dimension_mapping_names_with_current_ids,
    replace_dimension_names_with_current_ids,
)


logger = logging.getLogger(__name__)


def bulk_register(
    registry_manager: RegistryManager,
    registration_file: Path,
    base_data_dir: Path | None = None,
    base_repo_dir: Path | None = None,
    journal_file: Path | None = None,
):
    """Bulk register projects, datasets, and their dimensions. If any failure occurs, the code
    records successfully registered project and dataset IDs to a journal file and prints its
    filename to the console. Users can pass that filename with the --journal-file option to
    avoid re-registering those projects and datasets on subsequent attempts.

    The JSON/JSON5 filename must match the data model defined by this documentation:

    https://dsgrid.github.io/dsgrid/reference/data_models/project.html#dsgrid.config.registration_models.RegistrationModel
    """
    registration = RegistrationModel.from_file(registration_file)
    tmp_files = []
    if journal_file is None:
        journal_file = Path(f"journal__{uuid4()}.json5")
        journal = RegistrationJournal()
    else:
        journal = RegistrationJournal.from_file(journal_file)
        registration = registration.filter_by_journal(journal)
    failure_occurred = False
    try:
        return _run_bulk_registration(
            registry_manager,
            registration,
            tmp_files,
            base_data_dir,
            base_repo_dir,
            journal,
        )
    except Exception:
        failure_occurred = True
        raise
    finally:
        if failure_occurred and journal.has_entries():
            journal_file.write_text(journal.model_dump_json(indent=2), encoding="utf-8")
            logger.info(
                "Recorded successfully registered projects and datasets to %s. "
                "Pass this file to the `--journal-file` option of this command to skip those IDs "
                "on subsequent attempts.",
                journal_file,
            )
        elif journal_file.exists():
            journal_file.unlink()
            logger.info("Deleted journal file %s after successful registration.", journal_file)
        for path in tmp_files:
            path.unlink()


def _run_bulk_registration(
    mgr: RegistryManager,
    registration: RegistrationModel,
    tmp_files: list[Path],
    base_data_dir: Path | None,
    base_repo_dir: Path | None,
    journal: RegistrationJournal,
):
    user = getpass.getuser()
    project_mgr = mgr.project_manager
    dataset_mgr = mgr.dataset_manager
    dim_mgr = mgr.dimension_manager
    dim_mapping_mgr = mgr.dimension_mapping_manager

    if base_repo_dir is not None:
        for project in registration.projects:
            if not project.config_file.is_absolute():
                project.config_file = base_repo_dir / project.config_file
        for dataset in registration.datasets:
            if not dataset.config_file.is_absolute():
                dataset.config_file = base_repo_dir / dataset.config_file
        for dataset in registration.dataset_submissions:
            for field in (
                "dimension_mapping_file",
                "dimension_mapping_references_file",
            ):
                path = getattr(dataset, field)
                if path is not None and not path.is_absolute():
                    setattr(dataset, field, base_repo_dir / path)

    if base_data_dir is not None:
        for dataset in registration.datasets:
            if not dataset.dataset_path.is_absolute():
                dataset.dataset_path = base_data_dir / dataset.dataset_path

    for project in registration.projects:
        assert project.log_message is not None
        project_mgr.register(project.config_file, user, project.log_message)
        journal.add_project(project.project_id)

    for dataset in registration.datasets:
        config_file = None
        if dataset.replace_dimension_names_with_ids:
            mappings = map_dimension_names_to_ids(dim_mgr)
            orig = dataset.config_file
            config_file = orig.with_stem(orig.name + "__tmp")
            shutil.copyfile(orig, config_file)
            tmp_files.append(config_file)
            replace_dimension_names_with_current_ids(config_file, mappings)
        else:
            config_file = dataset.config_file

        assert dataset.log_message is not None
        dataset_mgr.register(
            config_file,
            user,
            dataset.log_message,
        )
        journal.add_dataset(dataset.dataset_id)

    for dataset in registration.dataset_submissions:
        refs_file = None
        if (
            dataset.replace_dimension_mapping_names_with_ids
            and dataset.dimension_mapping_references_file is not None
        ):
            dim_id_to_name = map_dimension_ids_to_names(mgr.dimension_manager)
            mappings = map_dimension_mapping_names_to_ids(dim_mapping_mgr, dim_id_to_name)
            orig = dataset.dimension_mapping_references_file
            refs_file = orig.with_stem(orig.name + "__tmp")
            shutil.copyfile(orig, refs_file)
            tmp_files.append(refs_file)
            replace_dimension_mapping_names_with_current_ids(refs_file, mappings)
        else:
            refs_file = dataset.dimension_mapping_references_file

        assert dataset.log_message is not None
        project_mgr.submit_dataset(
            dataset.project_id,
            dataset.dataset_id,
            user,
            dataset.log_message,
            dimension_mapping_file=dataset.dimension_mapping_file,
            dimension_mapping_references_file=refs_file,
            autogen_reverse_supplemental_mappings=dataset.autogen_reverse_supplemental_mappings,
        )
        journal.add_submitted_dataset(dataset.dataset_id, dataset.project_id)
