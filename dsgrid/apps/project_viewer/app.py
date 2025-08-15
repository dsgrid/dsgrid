import pandas as pd
import requests
from dash import Dash, dash_table, dcc, html, Input, Output, State
import dash_bootstrap_components as dbc

from dash.exceptions import PreventUpdate

from dsgrid.api.response_models import ListProjectsResponse


DSGRID_API_URL = "http://127.0.0.1:8000"

# Copied DataTable styles from https://gist.github.com/marcogoldin/8fc4c3945cef17ca38d55c4e17ebbbe6
STYLE_TABLE = {
    "fontFamily": '-apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,"Helvetica Neue",Arial,'
    '"Noto Sans",sans-serif,"Apple Color Emoji","Segoe UI Emoji","Segoe UI Symbol","Noto Color '
    'Emoji"'
}
STYLE_HEADER = {"backgroundColor": "white", "fontWeight": "bold", "padding": "0.75rem"}
STYLE_CELL = {
    "fontFamily": STYLE_TABLE["fontFamily"],
    "fontWeight": "400",
    "lineHeight": "1.5",
    "color": "#212529",
    "textAlign": "left",
    "whiteSpace": "normal",
    "height": "auto",
    "padding": "0.75rem",
    "border": "1px solid #dee2e6",
    "verticalAlign": "top",
}
STYLE_DATA_CONDITIONAL = [{"if": {"row_index": "odd"}, "backgroundColor": "#f8f9fa"}]

app = Dash(__name__, external_stylesheets=[dbc.themes.SPACELAB])
app.layout = dbc.Container(
    [
        dbc.Alert("dsgrid Project Viewer", color="info"),
        html.Div(
            [
                "URL: ",
                dcc.Input(
                    id="url_text",
                    value=DSGRID_API_URL,
                ),
                html.Label("Status:", style={"margin-left": "15px", "margin-right": "15px"}),
                dcc.Input(
                    id="status_text",
                    value="disconnected",
                ),
                dbc.Button(
                    "Connect",
                    outline=True,
                    color="primary",
                    id="connect_button",
                    n_clicks=0,
                    style={"margin-left": "15px"},
                ),
            ]
        ),
        html.Br(),
        html.Div(
            [
                "Select a project",
                dcc.Dropdown([], "", id="project_dd"),
            ],
        ),
        html.Br(),
        dbc.Button(
            "List dimensions",
            outline=True,
            color="primary",
            id="list_dimensions_button",
            n_clicks=0,
        ),
        html.Div(
            [
                dash_table.DataTable([], [], id="dimensions_table"),
                html.Div(id="dimensions_table_container"),
            ],
        ),
        html.Br(),
        html.H5("Dimension Records"),
        dcc.Input(
            id="dimension_name",
            value="",
            readOnly=True,
        ),
        html.Div(
            [
                dash_table.DataTable([], [], id="dimension_records_table"),
                html.Div(id="dimension_records_table_container"),
            ]
        ),
    ],
    className="m-5",
    # fluid=True,
    # Per the docs, this should be set to True. However, that causes a display issue where the
    # right part of the GUI is hidden and so you have to scroll horizontally to see it.
    # When passing the allowed parameters, like lg, xl, xxl, the width is too small.
    # Passing a non-supported string makes it work. This will almost certainly break in the future.
    fluid="invalidparameter",
)


@app.callback(
    Output("project_dd", "options"),
    Output("status_text", "value"),
    Input("connect_button", "n_clicks"),
    State("url_text", "value"),
)
def on_connect(n_clicks, url):
    if n_clicks is None:
        raise PreventUpdate
    return list_project_ids(url), "connected"


@app.callback(
    Output("project_dd", "value"),
    Input("project_dd", "options"),
)
def on_project_options_change(options):
    if options:
        return options[0]
    return ""


@app.callback(
    Output("dimensions_table_container", "children"),
    Input("list_dimensions_button", "n_clicks"),
    State("project_dd", "value"),
    State("url_text", "value"),
)
def on_list_dimensions(n_clicks, project_id, url):
    if n_clicks is None or project_id == "":
        raise PreventUpdate
    table = list_project_dimensions(project_id, url)
    return dash_table.DataTable(
        table,
        [{"name": x, "id": x} for x in table[0].keys()],
        id="dimensions_table",
        editable=False,
        filter_action="native",
        sort_action="native",
        row_selectable="single",
        selected_rows=[],
        style_table=STYLE_TABLE,
        style_header=STYLE_HEADER,
        style_cell=STYLE_CELL,
        style_data_conditional=STYLE_DATA_CONDITIONAL,
    )


@app.callback(
    Output("dimension_records_table_container", "children"),
    Output("dimension_name", "value"),
    Input("dimensions_table", "derived_viewport_selected_rows"),
    Input("dimensions_table", "derived_viewport_data"),
    State("url_text", "value"),
)
def on_list_dimension_records(row_indexes, row_data, url):
    if not row_indexes:
        raise PreventUpdate

    row_index = row_indexes[0]
    records = list_dimension_records(row_data[row_index]["dimension_id"], url)
    if not records:
        raise PreventUpdate

    df = pd.DataFrame.from_records(records)
    columns = []
    for column in records[0].keys():
        num_unique = df[column].nunique()
        new_name = f"{column} ({num_unique} unique)"
        columns.append({"name": new_name, "id": column})

    return (
        dash_table.DataTable(
            records,
            columns,
            id="dimension_records_table",
            editable=False,
            filter_action="native",
            sort_action="native",
            style_table=STYLE_TABLE,
            style_header=STYLE_HEADER,
            style_cell=STYLE_CELL,
            style_data_conditional=STYLE_DATA_CONDITIONAL,
        ),
        row_data[row_index]["name"],
    )


def list_project_ids(url):
    response = ListProjectsResponse(**check_request("projects", url))
    return [x.project_id for x in response.projects]


def list_project_dimensions(project_id, url):
    return check_request(f"projects/{project_id}/dimensions", url)["dimensions"]


def list_dimension_records(dimension_id, url):
    return check_request(f"dimensions/records/{dimension_id}", url)["records"]


def check_request(endpoint, url):
    target = f"{url}/{endpoint}"
    response = requests.get(target)
    if response.status_code != 200:
        msg = f"request to {target} failed: {response.status_code}"
        raise Exception(msg)
    return response.json()


if __name__ == "__main__":
    app.run(debug=True)
