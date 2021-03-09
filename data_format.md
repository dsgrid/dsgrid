Dsgrid Data Structure

Data Tables (x2)
	* "load_data": contains load time series by end use columns indexed by a data index
	* "load_data_lookup": contains the mapping from data index to subsector and geographic indices

Scaling Factor Table (x2) - optional
	* stores scaling factors for data disaggregation from one dimension to another.
	* "xxx":
	* "xxx":

Data Partitioning
	* data tables are stored as partitioned snappy parquet files
	* default partitioning is xxx MB/file before compression

Data Binning
	* 

dataset_dimension_mapping
	* defines the dimensions of input datasets
	* defaults: xx

project_dimension_mapping
	* defines the dimensions of data to output at the project level
	* defaults: xx

Metadata option for scaling factors
	* stores sectoral scaling factors as single numbers and other scaling factors of similar nature
	* can be looked up by xxx

dataset.toml
	* configuration file that holds all other metadata details