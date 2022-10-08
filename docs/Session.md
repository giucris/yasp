# Session

A session is a model that describes how Yasp should create the SparkSession.

Currently is defined as follow:

```scala

sealed trait SessionType extends Product with Serializable

object SessionType {
  final case object Local extends SessionType
  final case object Distributed extends SessionType
}

final case class Session(
    kind: SessionType,
    name: String,
    conf: Option[Map[String, String]] = None,
    withHiveSupport: Option[Boolean] = None,
    withDeltaSupport: Option[Boolean] = None,
    withCheckpointDir: Option[String] = None
)
```


* kind: valid values are **local** (for local runs) and **distributed** (for production run)
* name: the spark app name
* conf: Optional map, by default is considered empty, for spark session configuration
* withHiveSupport: Optional flag, by default is considered false, for activate the Hive support
* withDeltaSupport: Optional flag, by default is considered false, for activate Delta table support (i.e: **Delta catalog** and **Delta SparkSession extensions**)
* withCheckpointDir: Optional string, by default is considered empty, for configuring the checkpoint directory


An example of a full yaml configuration:

```yaml
session: 
  kind: local                           # Create a local session settings the master to local[*]
  name: my-session                      # Create a session with my-session as app-name
  conf:                                 # Spark session configuration map
    spark.sql.shuffle.partitions:100    # Spark sql shuffle partition configured to 100
  withHiveSupport: true                 # Enable hive support
  withDeltaSupport: true                # Enable Delta table support
  withCheckpointDir: /my/cp/directory/  # Set Spark checkpoint directory to /my/cp/directory
```
