# Apache Spark

Guide for using Apache Spark as dsgrid's distributed Ibis backend. Spark is optional — DuckDB is the default backend and is sufficient for local work and moderate-sized datasets. Use Spark when datasets or aggregations exceed what a single node can handle.

## Core Concepts

```{toctree}
:maxdepth: 1

overview
```

## How-Tos

- [How to Run dsgrid on Kestrel](../how_tos/run_on_kestrel)
- [How to Start a Spark Cluster on Kestrel](../how_tos/spark_cluster_on_kestrel)
