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
    withHiveSupport: Option[Boolean]  = None,
    withDeltaSupport: Option[Boolean] = None,
    withCheckpointDir: Option[String] = None
)
```

Fields:
* **kind** [**REQUIRED**]: Kind of SparkSession that will be created. Valid values are **local** and **distributed**, local for local usage and distributed for production usage.
* **name** [**REQUIRED**]: Define the SparkSession app name
* **conf** [**OPTIONAL**]: Map of strings, default is empty, for SparkSession configuration.
* **withHiveSupport** [**OPTIONAL**]: Boolean flag, default is false, for activating the Hive support on the SparkSession.
* **withDeltaSupport** [**OPTIONAL**]: Boolean flag, default is false, for activating Delta table support on the SparkSession (**Delta catalog** and **Delta SparkSessionExtensions**).
* **withCheckpointDir** [**OPTIONAL**]: String, default is empty, for configuring the SparkContext checkpoint directory.

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
