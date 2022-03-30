import os
import sys
from pathlib import Path
import logging
import getpass

from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.tests.common import (
    create_local_test_registry,
    replace_dimension_uuids_from_registry,
)

logger = logging.getLogger(__name__)

### set up registry for AEO data ###
def register_aeo_datasets(
    src_dir,
    registry_path=None,
    offline_mode=True,
) -> RegistryManager:
    """register AEO dimensions and dataset.

    Parameters
    ----------
    src_dir : Path
        Repo path containing source config files
        (e.g., "~/dsgrid-project-StandardScenarios/")
    registry_path : Path
        Path in which the registry will be created.
    offline_mode: bool
        Whether to register in offline mode.

    """
    user = getpass.getuser()
    log_message = "Initial registration"

    if registry_path is None:
        registry_path = os.environ.get("DSGRID_REGISTRY_PATH")
    if registry_path is None:
        raise ValueError(
            "You must define the path to registry in the env variable DSGRID_REGISTRY_PATH"
        )

    dataset_dir = Path("dsgrid_project/datasets/benchmark/aeo2021/reference")

    sectors = ["commercial"]  # <--- ["commercial", "industrial", "residential"]
    metrics_by_sector = {
        "commercial": [
            "End_Use_Growth_Factors"
        ],  # ["End_Use_Growth_Factors", "End_Uses", "Floor_Area", "Stock_Growth_Factors"],
        "industrial": ["End_Use_Growth_Factors", "End_Uses", "Floor_Area", "Stock_Growth_Factors"],
        "residential": [
            "End_Use_Growth_Factors",
            "End_Uses",
            "Floor_Area",
            "Stock_Growth_Factors",
            "Household_Units",
        ],
        "transportation": ["End_Uses", "Miles_Traveled"],
    }

    manager = RegistryManager.load(registry_path, offline_mode=offline_mode)
    dim_mgr = manager.dimension_manager

    for sector in sectors:
        metrics = metrics_by_sector[sector]
        for metric in metrics:
            sector_metric = Path(f"{sector}/{metric}")
            dim_mgr.register(
                src_dir / dataset_dir / sector_metric / "dimensions.toml", user, log_message
            )
            dataset_config_file = src_dir / dataset_dir / sector_metric / "dataset.toml"
            dataset_path = src_dir / dataset_dir / sector_metric / "data/load_data.csv"
            replace_dataset_path(dataset_config_file, dataset_path=dataset_path)
            replace_dimension_uuids_from_registry(registry_path, (dataset_config_file,))

            manager.dataset_manager.register(dataset_config_file, user, log_message)
            logger.info(f"dataset={sector_metric} registered successfully!\n")

    return manager


if __name__ == "__main__":
    """
    Usage:
    python register_aeo_datasets.py path_to_standard_scenarios_repo
    """
    if len(sys.argv) != 2:
        print(f"Usage: python {sys.argv[0]} <path_to_standard_scenarios_repo>")
        sys.exit(1)

    register_aeo_datasets(sys.argv[1])
