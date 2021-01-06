"""Spark helper functions"""

import multiprocessing

from pyspark.sql import SparkSession


def init_spark(name, mem="5gb", num_cpus=None):
    """Initialize a SparkSession."""
    if num_cpus is None:
        num_cpus = multiprocessing.cpu_count()

    return SparkSession.builder \
        .master('local') \
        .appName(name) \
        .config('spark.executor.memory', mem) \
        .config("spark.cores.max", str(num_cpus)) \
        .getOrCreate()
