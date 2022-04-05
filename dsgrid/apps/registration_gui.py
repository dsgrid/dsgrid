import getpass
import os
import sys
from pathlib import Path

from IPython.display import display, HTML
import ipywidgets as widgets
from pyspark.sql import SparkSession

from dsgrid.common import REMOTE_REGISTRY, LOCAL_REGISTRY
from dsgrid.registry.registry_manager import RegistryManager
from dsgrid.utils.spark import init_spark


class RegistrationGui:
    """Provides a UI for registering dsgrid projects and datasets."""

    def __init__(self):
        self._manager = None
        self._project_ids = ["all"]
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
        self._remote_path_text = widgets.Text(
            f"{REMOTE_REGISTRY}",
            description="Remote registry",
            layout=text_layout,
        )
        self._local_path_text = widgets.Text(
            str(LOCAL_REGISTRY),
            description="Local registry",
            layout=text_layout,
        )
        self._spark_cluster_text = widgets.Text(
            os.environ.get("SPARK_CLUSTER", "local mode"),
            description="Spark cluster",
            layout=text_layout,
        )
        self._online_mode_cbox = widgets.Checkbox(
            value=False,
            description="online mode",
        )
        self._online_mode_cbox.observe(self._on_online_click, names="value")
        self._sync_cbox = widgets.Checkbox(
            value=False,  # TODO: set to true when registry is fixed
            description="Sync pull",
        )
        self._load_btn = widgets.Button(description="Load registry")
        self._load_btn.on_click(self._on_load_click)
        self._register_project_btn = widgets.Button(description="Register project", disabled=True)
        self._register_project_btn.on_click(self._on_register_project_click)
        self._project_file_text = widgets.Text("", description="Project File")
        self._register_dataset_btn = widgets.Button(description="Register dataset", disabled=True)
        self._register_dataset_btn.on_click(self._on_register_dataset_click)
        self._dataset_file_text = widgets.Text("", description="Dataset File")
        self._log_message_text = widgets.Text(
            "", description="Registration log message", layout=widgets.Layout(width="500px")
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
        self._dimensions_filter_text = widgets.Text(
            tooltip="Example: Type == geography",
            description="Filter dimensions",
            value="",
        )
        self._project_dimensions_filter_dd = widgets.Dropdown(
            options=self._project_ids,
            value=self._project_ids[0],
            description="Filter dimensions by project:",
            disabled=False,
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
            (self._remote_path_text, self._local_path_text, self._spark_cluster_text)
        )
        options_box = widgets.VBox((self._online_mode_cbox, self._sync_cbox))

        register_project_box = widgets.HBox((self._register_project_btn, self._project_file_text))
        register_dataset_box = widgets.HBox((self._register_dataset_btn, self._dataset_file_text))
        register_box = widgets.VBox(
            (register_project_box, register_dataset_box, self._log_message_text)
        )

        show_dims_box = widgets.HBox(
            (
                self._show_dimensions_btn,
                self._dimensions_filter_text,
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
        self._register_dataset_btn.disabled = False
        self._show_projects_btn.disabled = False
        self._show_datasets_btn.disabled = False
        self._show_dimensions_btn.disabled = False
        self._show_dimension_mappings_btn.disabled = False
        self._project_dimensions_filter_dd.disabled = False
        self._project_dimensions_filter_dd.options = self._project_ids
        out = widgets.Output()
        with out:
            self._on_show_projects_click(self._show_projects_btn)
            self._on_show_datasets_click(self._show_datasets_btn)
        out.clear_output()

    def _on_online_click(self, _):
        # Syncing is always enabled when in online mode.
        self._sync_cbox.disabled = self._online_mode_cbox.value

    def _on_load_click(self, _):
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
        if sync and not online:
            # This exists only to sync data locally.
            RegistryManager.load(
                path=self._local_path_text.value,
                remote_path=self._remote_path_text.value,
                offline_mode=False,
                user=getpass.getuser(),
            )
        self._manager = RegistryManager.load(
            path=self._local_path_text.value,
            remote_path=self._remote_path_text.value,
            offline_mode=not online,
            user=getpass.getuser(),
        )
        self._project_ids[1:] = self._manager.project_manager.list_ids()
        self._enable_manager_actions()

    def _on_register_project_click(self, _):
        project_file = self._project_file_text.value
        if project_file == "":
            print("project_file cannot be empty", file=sys.stderr)
            return
        self._manager.project_manager.register(
            project_file, submitter=getpass.getuser(), log_message=self._log_message_text.value
        )
        self._post_registration_handling()

    def _on_register_dataset_click(self, _):
        dataset_file = self._dataset_file_text.value
        if dataset_file == "":
            print("dataset_file cannot be empty", file=sys.stderr)
            return
        self._manager.dataset_manager.register(
            dataset_file, submitter=getpass.getuser(), log_message=self._log_message_text.value
        )
        self._post_registration_handling()

    def _registration_pre_check(self):
        log_message = self._log_message_text.value
        if log_message == "":
            print("log_message cannot be empty", file=sys.stderr)
            return

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
        if project_id == "all":
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
