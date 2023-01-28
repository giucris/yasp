# YaspPlan

A `YaspPlan` is a model that define your ETL/EL jobs in terms of sources, processes and sinks.

Currently is defined as follow:

```scala
case class YaspPlan(
  sources: Seq[YaspSource],     // A Sequence of YaspSource
  processes: Seq[YaspProcess],  // A Sequence of YaspProcess
  sinks: Seq[YaspSink]          // A Sequence of YaspSink
)
```

* **sources** [**REQUIRED**]: A List of `YaspSource`, default is empty
* **processes** [**REQUIRED**]: A List of `YaspProcess`, default is empty
* **sinks** [**REQUIRED**]: A List of `YaspProcess`, default is empty

## YaspSource

A `YaspSource` is a model that define a data source.

Currently is defined as follow:

```scala
case class YaspSource(
  id: String,               // Unique ID to internally identify the action
  dataset: String,          // Dataset name, used to register the in memory dataset
  source: Source,           // Source Sum Type
  partitions: Option[Int],  // Optional number of partitions 
  cache: Option[CacheLayer] // Optional CacheLayer that will be used to cache resulting data  
)
```

* **id** [**REQUIRED**]: String ID to internally identify the data
* **source** [**REQUIRED**]: A Source Sum Type. Valid values are `Format` and `HiveTable`.
* **partitions** [**OPTIONAL**]: A number of partitions used to repartition the data.
* **cache** [**OPTIONAL**]: An optional CacheLayer Sum Type. Valid values are:
    * `Memory|memory|MEMORY`,
    * `Disk|disk|DISK`
    * `MemoryAndDisk|memodyanddisk|MEMORYANDDISK|memory_and_disk|MEMORY_AND_DISK`
    * `MemorySer|memoryser|MEMORYSER|memory_ser|MEMORY_SER`
    * `MemoryAndDiskSer|memoryanddiskser|MEMORYANDDISKSER|memory_and_disk_ser|MEMORY_AND_DISK_SER`
    * `Checkpoint|checkpoint|CHECKPOINT`

Each `YaspSource` are loaded via a `YaspLoader` that read, repartition and cache the data to a specific `CacheLayer` and
register it as a temporary table with the id provided.

An example of a full yaml configuration with Spark format:

```yaml
source: 
  id: my_source                  # Source id
  source:                        # Source configuration
    dataset: mydata              # Source dataset name
    format: jdbc                 # Standard jdbc Spark format
    options:                     # Standard jdbc Spark format option
        url: my-jdbc-endpoint    # Standard jdbc Spark url config
        user: my-user            # Standard jdbc Spark user config
        password: my-pwd         # Standard jdbc Spark pwd config  
        dbTable: db.table        # Standard jdbc Spark dbTable config
    partitions: 500              # Partitions number
    cache: checkpoint            # Cache layer to checkpoint
```

An example of a full yaml configuration with Hive table

```yaml
source: 
  id: my_source              # Source id
  source:                    # Source configuration
    dataset: mydata          # Source dataset name
    table: my_hive_table     # Hive table name  
  partitions: 500            # Partitions number
  cache: checkpoint          # Cache layer to checkpoint
```

## YaspProcess

A `YaspProcess` is a model that define a data process operation.

Currently is defined as follow:

```scala
case class YaspProcess(
  id: String,                 // Unique ID to internally identify the action
  dataset: String,            // Dataset name used to register the outcome of the process
  process: Process,           // Process Sum Type
  partitions: Option[Int],    // Optional number of partitions
  cache: Option[CacheLayer]   // Optional CacheLayer that will be used to cache resulting dataframe
)
```

Each `YaspProcess` are executed via a `YaspProcessor` that execute the `Process`, repartition and cache the data to a
specific `CacheLayer` and register it as a temporary table with the id provided.

An example of a full yaml configuration with Hive table

```yaml
process: 
  id: my_source         # Process id
  dataset: mydata       # Dataset name
  process:              # Process field, will contains all the Process configuration to transform the data
    query: >-           # A sql process configuration
      SELECT * 
      FROM my_csv  
  partitions: 500        # Partitions number
  cache: checkpoint      # Cache layer to checkpoint
```

## YaspSink

A `YaspSink` define a data output operation.

```scala
case class YaspSink(
  id: String,       // Unique ID.
  dataset: String,  // Name of the dataset to sink.
  dest: Dest        // Dest Sum Type
)
```

Each `YaspSink` are executed via a `YaspWriter` that retrieve the specific data using the provided `id` and write them
to the specified destination.

An example of a full yaml configuration with Spark format:

```yaml
dest: 
  id: my_source                  # Source id
  dataset: mydata                # Name of the dataset to sink
  dest:                          # Source configuration
    format: csv                  # Standard csv Spark format
    options:                     # Standard csv Spark format option
        header: 'true'           # Standard csv Spark url config
        sep: '|'                 # Standard csv Spark user config
        path: my/output/path     # Standard csv Spark dbTable config
    partitionBy:                 # PartitionBy columns configuration
        - nation                 
        - city
    mode: overwrite              # Standard Spark SaveMode
```

An example of a full yaml configuration with HiveFormat:

```yaml
dest: 
  id: my_source                  # Source id
  dataset: mytable               # Name of the dataset to sink
  dest:                          # Source configuration
    table: my_hive_table         # Hive table name
    mode: overwrite              # Standard Spark SaveMode
```