from __future__ import annotations

from typing import Any

import pandas as pd

EVENT_METADATA: list[dict[str, Any]] = [
    {
        "Event": "Get Source Connection",
        "Meaning": "Power BI establishes or reuses a connection to the data source (DirectQuery).",
        "order": 0,
        "class": "Connect",
        "responsibility": "System",
    },
    {
        "Event": "Visual Container Lifecycle",
        "Meaning": (
            "The top-level visual is initialized and processed; child steps occur under this."
        ),
        "order": 1,
        "class": "Initiation and tracker",
        "responsibility": "Client",
    },
    {
        "Event": "Resolve Parameters",
        "Meaning": (
            "Evaluates parameters used in the visual (filters, slicers, dynamic expressions) "
            "before running queries."
        ),
        "order": 2,
        "class": "Pre-query",
        "responsibility": "System",
    },
    {
        "Event": "Query",
        "Meaning": (
            "Logical query to fetch data from the model (DAX, DirectQuery, or cached result)."
        ),
        "order": 3,
        "class": "Logical query",
        "responsibility": "System",
    },
    {
        "Event": "Query Generation",
        "Meaning": "Translates the visual / filter context into a DAX query for the engine.",
        "order": 4,
        "class": "Logical query",
        "responsibility": "System",
    },
    {
        "Event": "Query Pending",
        "Meaning": "Time spent waiting for a query to be queued/scheduled in the engine.",
        "order": 5,
        "class": "Scheduling",
        "responsibility": "System",
    },
    {
        "Event": "Execute Semantic Query",
        "Meaning": "Executes the DAX query against the semantic model (DSE / Tabular).",
        "order": 6,
        "class": "Execution",
        "responsibility": "Formula Engine / VertiPaq",
    },
    {
        "Event": "Execute DAX Query",
        "Meaning": "Evaluates the DAX query; may include measures, aggregations, and computations.",
        "order": 7,
        "class": "Execution",
        "responsibility": "Formula Engine",
    },
    {
        "Event": "Execute Query",
        "Meaning": "Roll-up for query execution (covers DAX and DirectQuery depending on mode).",
        "order": 7.5,
        "class": "Execution",
        "responsibility": "Formula Engine / External Source",
    },
    {
        "Event": "Execute Direct Query",
        "Meaning": "DirectQuery only: sends source-native queries to the underlying database.",
        "order": 8,
        "class": "Execution",
        "responsibility": "External Source",
    },
    {
        "Event": "Parse Query Result",
        "Meaning": (
            "Parses results returned from the engine or DirectQuery before use in the visual."
        ),
        "order": 9,
        "class": "Result handling",
        "responsibility": "Formula Engine / Client",
    },
    {
        "Event": "Serialize Rowset",
        "Meaning": "Serializes query results for transfer from engine to client / visual.",
        "order": 10,
        "class": "Result handling",
        "responsibility": "Formula Engine / Client",
    },
    {
        "Event": "Data View Transform",
        "Meaning": (
            "Transforms returned data for the visual (formatting, reshaping, computed columns, "
            "etc.)."
        ),
        "order": 11,
        "class": "Visual",
        "responsibility": "Client",
    },
    {
        "Event": "Render",
        "Meaning": "Renders the visual on the report canvas (drawing charts, tables, etc.).",
        "order": 12,
        "class": "Visual",
        "responsibility": "Client",
    },
    {
        "Event": "Visual Update",
        "Meaning": "Updates a visual (refresh due to data changes or interaction).",
        "order": 13,
        "class": "Visual",
        "responsibility": "Client",
    },
    {
        "Event": "Visual Update Async",
        "Meaning": "Asynchronous background updates to visuals (non-blocking).",
        "order": 14,
        "class": "Visual",
        "responsibility": "Client",
    },
]


def event_metadata_frame() -> pd.DataFrame:
    """Default event metadata (glossary) as a DataFrame."""
    return pd.DataFrame(EVENT_METADATA).sort_values("order").reset_index(drop=True)
