import copy
import getpass
import logging
import os
import sys
from pathlib import Path

from IPython.display import display, HTML
import ipywidgets as widgets

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.exceptions import DSGBaseException
from dsgrid.registry.registry_database import DatabaseConnection
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.loggers import setup_logging
from dsgrid.spark.types import SparkSession
from dsgrid.utils.spark import init_spark

SS_PROJECT = "https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/project.json5"
RS_DATASET = "https://github.com/dsgrid/dsgrid-project-StandardScenarios/blob/main/dsgrid_project/datasets/modeled/resstock/dataset.json5"

logger = logging.getLogger(__name__)


class RegistrationGui:
    """Provides a UI for registering dsgrid projects and datasets."""

    DEFAULTS = {
        "remote_registry": REMOTE_REGISTRY,
        "local_registry": LOCAL_REGISTRY,
        "project_file": "",
        "dataset_file": "",
        "dataset_path": "",
        "dimension_mapping_file": "",
        "dimensions_filter": "",
        "log_file": Path(os.environ.get("DSGRID_LOG_FILE_PATH", ".")) / "dsgrid.log",
        "log_message": "",
        "spark_cluster": os.environ.get("SPARK_CLUSTER", "local mode"),
    }

    def __init__(self, defaults=None):
        self._manager = None
        self._defaults = copy.deepcopy(self.DEFAULTS)
        if defaults is not None:
            self._defaults.update(defaults)
        self._project_ids = [""]
        self._make_widgets()
        self._display_widgets()
        self._tables_out = widgets.Output()

    @property
    def manager(self):
        return self._manager

    @property
    def dimension_manager(self):
        return self._manager.dimension_manager

    @property
    def dimension_mapping_manager(self):
        return self._manager.dimension_mapping_manager

    @property
    def dataset_manager(self):
        return self._manager.dataset_manager

    @property
    def project_manager(self):
        return self._manager.project_manager

    def _make_widgets(self):
        self._main_label = widgets.HTML("<b>dsgrid Registration Tool</b>")
        text_layout = widgets.Layout(width="400px")
        button_layout = widgets.Layout(width="200px")
        self._remote_path_text = widgets.Text(
            str(self._defaults["remote_registry"]),
            description="Remote registry",
            layout=text_layout,
        )
        self._local_path_text = widgets.Text(
            str(self._defaults["local_registry"]),
            description="Local registry",
            layout=text_layout,
        )
        self._spark_cluster_text = widgets.Text(
            self._defaults["spark_cluster"],
            description="Spark cluster",
            layout=text_layout,
        )
        log_file = self._defaults["log_file"]
        # TODO: setup detection of changes to this text box and reconfigure logging
        self._log_file_text = widgets.Text(
            str(log_file),
            description="Log file",
            layout=text_layout,
        )
        self._online_mode_cbox = widgets.Checkbox(
            value=False,
            description="Online mode",
        )
        self._online_mode_cbox.observe(self._on_online_click, names="value")
        self._sync_cbox = widgets.Checkbox(
            value=True,
            description="Sync pull",
        )
        self._load_btn = widgets.Button(description="Load registry", layout=button_layout)
        self._load_btn.on_click(self._on_load_click)
        self._register_project_btn = widgets.Button(
            description="Register project", disabled=True, layout=button_layout
        )
        self._register_project_btn.on_click(self._on_register_project_click)
        self._project_file_text = widgets.Text(
            str(self._defaults["project_file"]),
            description="Project File",
            placeholder="project.json5",
        )
        self._project_file_ex = widgets.HTML(
            f"<a href={SS_PROJECT} target='_blank'>Example: Standard Scenarios</a>"
        )
        self._register_and_submit_dataset_btn = widgets.Button(
            description="Register and submit dataset", disabled=True, layout=button_layout
        )
        self._register_and_submit_dataset_btn.on_click(self._on_register_and_submit_dataset_click)
        self._dataset_file_ex = widgets.HTML(
            f"<a href={RS_DATASET} target='_blank'>Example: ResStock</a>"
        )
        self._dataset_file_text = widgets.Text(
            str(self._defaults["dataset_file"]),
            description="Dataset File",
            placeholder="dataset.json5",
        )
        self._dataset_path_text = widgets.Text(
            self._defaults["dataset_path"],
            description="Dataset Path",
            placeholder="load_data_path",
        )
        self._dimension_mapping_label = widgets.HTML("Dimension mapping file")
        self._dimension_mapping_text = widgets.Text(
            str(self._defaults["dimension_mapping_file"]), placeholder="dimension_mappings.json5"
        )
        self._dataset_project_id_dd = widgets.Dropdown(
            description="Project ID",
            options=self._project_ids,
            value=self._project_ids[0],
            disabled=True,
        )
        self._log_message_label = widgets.HTML("Registration log message")
        self._log_message_text = widgets.Text(
            self._defaults["log_message"], layout=widgets.Layout(width="400px")
        )
        self._show_projects_btn = widgets.Button(
            disabled=True,
            description="Show projects",
            tooltip="Display a table showing all registered projects",
        )
        self._show_projects_btn.on_click(self._on_show_projects_click)
        self._show_datasets_btn = widgets.Button(
            disabled=True,
            description="Show datasets",
            tooltip="Display a table showing all registered datasets",
        )
        self._show_datasets_btn.on_click(self._on_show_datasets_click)
        self._show_dimensions_btn = widgets.Button(
            disabled=True,
            description="Show dimensions",
            tooltip="Display a table showing all registered dimensions",
        )
        self._show_dimensions_btn.on_click(self._on_show_dimensions_click)
        self._dim_filter_message_text = widgets.HTML("Filter dimensions")
        self._dimensions_filter_text = widgets.Text(
            self._defaults["dimensions_filter"], placeholder="Type == geography"
        )
        self._project_dimensions_filter_text = widgets.HTML("Filter dimensions by project")
        self._project_dimensions_filter_dd = widgets.Dropdown(
            options=self._project_ids,
            value=self._project_ids[0],
            disabled=True,
        )
        self._show_dimension_mappings_btn = widgets.Button(
            disabled=True,
            description="Show mappings",
            tooltip="Display a table showing all registered dimension mappings",
        )
        self._show_dimension_mappings_btn.on_click(self._on_show_dimension_mappings_click)
        self._reset_tables_btn = widgets.Button(description="Reset tables")
        self._reset_tables_btn.on_click(self._reset_tables_click)
        self._reset_btn = widgets.Button(description="Reset all")
        self._reset_btn.on_click(self._on_reset_click)

        # Disabling because these tables are not well-formed.
        # self._project_table = widgets.HTML(value="", description="Projects")
        # self._dataset_table = widgets.HTML(value="", description="Datasets")
        # self._dimension_table = widgets.HTML(value="", description="Dimensions")
        # self._dimension_mapping_table = widgets.HTML(value="", description="Dimension Mappings")

    def _display_widgets(self):
        registry_box = widgets.VBox(
            (
                self._remote_path_text,
                self._local_path_text,
                self._spark_cluster_text,
                self._log_file_text,
            )
        )
        options_box = widgets.VBox((self._online_mode_cbox, self._sync_cbox))

        register_project_box = widgets.HBox(
            (self._register_project_btn, self._project_file_text, self._project_file_ex)
        )
        register_and_submit_dataset_box = widgets.HBox(
            (
                self._register_and_submit_dataset_btn,
                widgets.VBox(
                    (
                        widgets.HBox((self._dataset_file_text, self._dataset_file_ex)),
                        self._dataset_path_text,
                        widgets.HBox(
                            (self._dimension_mapping_label, self._dimension_mapping_text)
                        ),
                        self._dataset_project_id_dd,
                    ),
                ),
            ),
        )
        log_box = widgets.HBox((self._log_message_label, self._log_message_text))
        register_box = widgets.VBox(
            (register_project_box, register_and_submit_dataset_box, log_box)
        )

        show_dims_box = widgets.HBox(
            (
                self._show_dimensions_btn,
                self._dim_filter_message_text,
                self._dimensions_filter_text,
                self._project_dimensions_filter_text,
                self._project_dimensions_filter_dd,
            )
        )
        show_box = widgets.VBox(
            (
                self._show_projects_btn,
                self._show_datasets_btn,
                show_dims_box,
                self._show_dimension_mappings_btn,
                # self._project_table,
                # self._dataset_table,
                # self._dimension_table,
                # self._dimension_mapping_table,
                self._reset_tables_btn,
            )
        )

        display(
            self._main_label,
            widgets.HBox((registry_box, options_box)),
            self._load_btn,
            register_box,
            show_box,
            self._reset_btn,
        )

    def _enable_manager_actions(self):
        self._register_project_btn.disabled = False
        self._register_and_submit_dataset_btn.disabled = False
        self._dataset_project_id_dd.disabled = False
        self._show_projects_btn.disabled = False
        self._show_datasets_btn.disabled = False
        self._show_dimensions_btn.disabled = False
        self._show_dimension_mappings_btn.disabled = False
        self._project_dimensions_filter_dd.disabled = False
        self._update_project_ids()
        out = widgets.Output()
        with out:
            self._on_show_projects_click(self._show_projects_btn)
            self._on_show_datasets_click(self._show_datasets_btn)
        out.clear_output()

    def _on_online_click(self, _):
        # Syncing is always enabled when in online mode.
        if self._online_mode_cbox.value:
            self._sync_cbox.value = True
        self._sync_cbox.disabled = self._online_mode_cbox.value

    def _on_load_click(self, _):
        # TODO: We should log to an Output widget that gets updated periodically.
        logger = setup_logging(__name__, self._log_file_text.value, mode="a")
        if (
            self._spark_cluster_text.value not in ("local mode", "")
            and SparkSession.getActiveSession() is None
        ):
            os.environ["SPARK_CLUSTER"] = self._spark_cluster_text.value
            out = widgets.Output()
            with out:
                init_spark()
            out.clear_output()

        sync = self._sync_cbox.value
        online = self._online_mode_cbox.value
        conn = DatabaseConnection()
        try:
            if sync and not online:
                # This exists only to sync data locally.
                RegistryManager.load(
                    conn,
                    remote_path=self._remote_path_text.value,
                    offline_mode=False,
                    user=getpass.getuser(),
                )
            self._manager = RegistryManager.load(
                conn,
                remote_path=self._remote_path_text.value,
                offline_mode=not online,
                user=getpass.getuser(),
            )
        except DSGBaseException:
            logger.exception("Failed to load registry %s", self._local_path_text.value)
            return

        self._enable_manager_actions()

    def _update_project_ids(self):
        self._project_ids[1:] = self._manager.project_manager.list_ids()
        self._project_dimensions_filter_dd.options = self._project_ids
        self._project_dimensions_filter_dd.value = self._project_ids[0]
        self._dataset_project_id_dd.options = self._project_ids
        self._dataset_project_id_dd.value = self._project_ids[0]

    def _on_register_project_click(self, _):
        project_file = Path(self._project_file_text.value)
        if str(project_file) == "":
            print("project_file cannot be empty", file=sys.stderr)
            return
        if not self._registration_pre_check():
            return
        try:
            self._manager.project_manager.register(
                project_file, submitter=getpass.getuser(), log_message=self._log_message_text.value
            )
        except DSGBaseException:
            logger.exception("Failed to register project %s", project_file)
            return

        self._update_project_ids()
        self._post_registration_handling()

    def _on_register_and_submit_dataset_click(self, _):
        dataset_file = Path(self._dataset_file_text.value)
        if str(dataset_file) == "":
            print("dataset_file cannot be empty", file=sys.stderr)
            return
        dataset_path = Path(self._dataset_path_text.value)
        if str(dataset_path) == "":
            print("dataset_path cannot be empty", file=sys.stderr)
            return
        dimension_mapping_file = Path(self._dimension_mapping_text.value)
        if str(dimension_mapping_file) == "":
            dimension_mapping_file = None
        project_id = self._dataset_project_id_dd.value
        if project_id == "":
            print("project_id cannot be empty", file=sys.stderr)
            return
        if not self._registration_pre_check():
            return
        try:
            self._manager.project_manager.register_and_submit_dataset(
                dataset_file,
                dataset_path,
                project_id,
                dimension_mapping_file=dimension_mapping_file,
                submitter=getpass.getuser(),
                log_message=self._log_message_text.value,
            )
        except DSGBaseException:
            logger.exception("Failed to register and submit dataset %s", dataset_file)
            return

        self._post_registration_handling()
        self._update_project_ids()

    def _registration_pre_check(self):
        log_message = self._log_message_text.value
        if log_message == "":
            print("log_message cannot be empty", file=sys.stderr)
            return False
        return True

    def _post_registration_handling(self):
        self._log_message_text.value = ""

    def _on_show_projects_click(self, _):
        table = self._manager.project_manager.show(return_table=True)
        # self._project_table.value = table.get_html_string()
        self._display_table("Projects", table)

    def _on_show_datasets_click(self, _):
        table = self._manager.dataset_manager.show(return_table=True)
        # self._dataset_table.value = table.get_html_string()
        self._display_table("Datasets", table)

    def _on_show_dimensions_click(self, _):
        filters = [self._dimensions_filter_text.value]
        if filters == [""]:
            filters = None
        project_id = self._project_dimensions_filter_dd.value
        if project_id == "":
            dimension_ids = None
        else:
            project_config = self._manager.project_manager.get_by_id(project_id)
            dimension_ids = {x.id for x in project_config.base_dimensions}
            for key in project_config.supplemental_dimensions:
                dimension_ids.add(key.id)

        table = self._manager.dimension_manager.show(
            filters=filters, dimension_ids=dimension_ids, return_table=True
        )
        self._display_table("Dimensions", table)

    def _display_table(self, name, table):
        self._tables_out.clear_output()
        self._tables_out = widgets.Output()
        with self._tables_out:
            display(HTML(f"<b>{name}</b>"))
            display(HTML(table.get_html_string()))
        display(self._tables_out)

    def _on_show_dimension_mappings_click(self, _):
        table = self._manager.dimension_mapping_manager.show(return_table=True)
        # self._dimension_mapping_table.value = table.get_html_string()
        self._display_table("Dimension Mappings", table)

    def _reset_tables_click(self, _):
        # self._project_table.value = ""
        # self._dataset_table.value = ""
        # self._dimension_table.value = ""
        # self._dimension_mapping_table.value = ""
        self._tables_out.clear_output()

    def _on_reset_click(self, _):
        for val in self.__dict__.values():
            if isinstance(val, widgets.Widget):
                val.close_all()
        self._make_widgets()
        self._display_widgets()
        self._enable_manager_actions()
