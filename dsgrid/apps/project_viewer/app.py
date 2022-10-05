import pandas as pd
import requests
from dash import Dash, dash_table, dcc, html, Input, Output, State
from dash.exceptions import PreventUpdate

from dsgrid.api.response_models import ListProjectsResponse


DSGRID_API_URL = "http://127.0.0.1:8000"

app = Dash(__name__)

app.layout = html.Div(
    children=[
        html.H1(children="dsgrid Project Viewer", className="app-header"),
        html.Div(
            [
                "dsgrid URL: ",
                dcc.Input(
                    id="url_text",
                    value=DSGRID_API_URL,
                ),
                "Status: ",
                dcc.Input(
                    id="status_text",
                    value="disconnected",
                ),
            ],
        ),
        html.Button("Connect", id="connect_button", n_clicks=0),
        html.Br(),
        html.H5("Select a project"),
        dcc.Dropdown([], "", id="project_dd"),
        html.Br(),
        html.Button("List dimensions", id="list_dimensions_button", n_clicks=0),
        html.Br(),
        html.Div(
            [
                dash_table.DataTable([], [], id="dimensions_table"),
                html.Div(id="dimensions_table_container"),
            ],
        ),
        html.Br(),
        html.Div(
            [
                html.H5("Dimension Records", className="app-header"),
                dcc.Input(
                    id="dimension_display_name",
                    value="",
                    readOnly=True,
                ),
                html.Div(
                    [
                        dash_table.DataTable([], [], id="dimension_records_table"),
                        html.Div(id="dimension_records_table_container"),
                    ],
                ),
            ],
        ),
    ],
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
        style_cell={"textAlign": "left", "padding": "5px"},
        style_header={
            "backgroundColor": "white",
            "fontWeight": "bold",
            "border": "1px solid grey",
        },
        style_data_conditional=[
            {
                "if": {"row_index": "odd"},
                "backgroundColor": "rgb(220, 220, 220)",
            }
        ],
        style_data={
            "border": "1px solid grey",
            "width": "100px",
            "minWidth": "100px",
            "maxWidth": "100px",
            "overflow": "hidden",
            "textOverflow": "ellipsis",
        },
    )


@app.callback(
    Output("dimension_records_table_container", "children"),
    Output("dimension_display_name", "value"),
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
            style_cell={"textAlign": "left", "padding": "5px"},
            style_header={
                "backgroundColor": "white",
                "fontWeight": "bold",
                "border": "1px solid grey",
            },
            style_data_conditional=[
                {
                    "if": {"row_index": "odd"},
                    "backgroundColor": "rgb(220, 220, 220)",
                }
            ],
            style_data={
                "border": "1px solid grey",
                "width": "100px",
                "minWidth": "100px",
                "maxWidth": "100px",
                "overflow": "hidden",
                "textOverflow": "ellipsis",
            },
        ),
        row_data[row_index]["display_name"],
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
        raise Exception(f"request to {target} failed: {response.status_code}")
    return response.json()


if __name__ == "__main__":
    app.run_server(debug=True)
