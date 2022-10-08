# YaspPlan

A `YaspPlan` is a model that define your ETL/EL jobs in terms of sources, processes and sinks.

Currently is defined as follow: 
```scala
case class YaspPlan(
  sources: Seq[YaspSource], // A Sequence of YaspSource
  processes: Seq[YaspProcess], // A Sequence of YaspProcess
  sinks: Seq[YaspSink] // A Sequence of YaspSink
)
```

## YaspSource
A `YaspSource` define a data source. 

Currently is defined as follow: 
```scala
case class YaspSource(
  id: String,               // Unique ID to internally identify the resulting dataframe
  source: Source,           // Source Sum Type
  partitions: Option[Int],  // Optional number of partitions 
  cache: Option[CacheLayer] // Optional cache layer that will be used to cache resulting dataframe  
)
```

Each `YaspSource` are loaded via a `YaspLoader` that read, repartition and cache the data to a specific `CacheLayer` and register it as a temporary table with the id provided.

## YaspProcess 

A `YaspProcess` define a data process operation.

```scala
case class YaspProcess(
  id: String,                 // Unique ID to internally identify the resulting dataframe
  process: Process,           // Process Sum Type
  partitions: Option[Int],    // Optional number of partitions
  cache: Option[CacheLayer]   // Optional cache layer that will be used to cache resulting dataframe
)
```

Each `YaspProcess` are executed via a `YaspProcessor` that execute the `Process`, repartition and cache the data to a specific `CacheLayer` and register it as a temporary table with the
id provided.

## YaspSink

A `YaspSink` define a data output operation. 

```scala
case class YaspSink(
  id: String,       // Unique ID of the dataframe that should be write out.
  dest: Dest        // Dest Sum Type
)
```

Each `YaspSink` are executed via a `YaspWriter` that retrieve the specific data using the provided `id` and write them to the specified destination.
