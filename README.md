# yasp

[![ci](https://github.com/giucris/yasp/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/giucris/yasp/actions/workflows/ci.yml)

Yet Another SPark Framework

An easy and lightweight tool for data engineering process built on top of Apache Spark.

## Introduction

Yasp was originally created just for fun and to help data engineers (I am one of them) working with Apache Spark to
reduce their pipeline development time by using a no-code/less-code approach.

It is written in **Scala (2.11.12)** on top of **Apache Spark (2.4.7)** and managed as an **SBT (1.4.9)** multi module
project.

## Getting Started

### Prerequisites

* JDK 8 Installed
* SBT Installed
* Windows user only: This is an Apache Spark based framework to run it locally you should configure the hadoop winutils
  on your laptop. Check [here](https://github.com/steveloughran/winutils) for more details.

### Local Execute

* checkout yasp: `git clone https://github.com/giucris/yasp.git`
* run yasp test: `sbt clean test`
* build yasp : `sbt assembly`
* all in one: `bash ci.sh`

## Usage

Yasp provide 3 layer of abstraction over spark framework.

* **YaspApp** provide the highest possible level of abstraction for your ETL job and come with an executable main class.
  Allow you to manage complex big data etl job with a simple yml file
* **YaspService** provide an domain specific language for your ETL job.
* **YaspCore** provide some spark primitives useful for yasp operations.

### YaspApp

You can use the YaspApp module as an executable binary or just as a library. Add the yasp-app reference to your
dependencies into your `build.sbt`or `pom.xml` file and then start using it.

#### YaspApp as library

YaspApp module provide an high level of abstraction around an etl job. 

YaspApp provide a way to define your etl in a pure descriptive way with a simple yml file.

For example:
```scala
object MyUsersByCitiesReport with ParserSupport{
  
  def main(args: Array[String]): Unit={
      YaspService().run(
        parseYaml[YaspExecution](
          """session:
            |  kind: Local
            |  name: example-app
            |  conf: {}
            |plan:
            |  sources:
            |    - id: users
            |      source:
            |        Csv:
            |          path: users.csv
            |    - id: addresses
            |      source:
            |        Json:
            |          path: addresses.jsonl
            |  processes:
            |    - id: user_with_address
            |      process:
            |        Sql:
            |          query: SELECT u.name,u.surname,a.address,a.city,a.country FROM users u JOIN addresses a ON u.id = a.user_id
            |  sinks:
            |    - id: user_with_address
            |      dest:
            |        Parquet:
            |          path: user_with_address
            |          partitionBy:
            |            - country
            |""".stripMargin    
        )
      )
  }
}
```

Take a look at the YaspService and YaspCore section for more detail


### YaspService

You can use YaspService just as a library. Add the yasp-service reference to your dependencies into your `build.sbt`
or `pom.xml` file and then start using it.

#### YaspExecution and YaspPlan

The main component of the YaspService module are `YaspExecution`, `YaspPlan` and of course the `YaspService`.

A YaspExecution is a model that define an e2e ETL job executed by the `YaspService`.

A YaspExecution define a `SessionConf` that describe how the `SparkSession` will be created and a `YaspPlan` that
describe all data operations within the ETL job.

```scala
case class Session(
  kind: SessionType, // A SumType with two possible value Local (for local session) Distributed (for cluster session)
  name: String, // Spark application name
  config: Map[String, String] // Spark session configuration
)

case class YaspPlan(
  sources: Seq[YaspSource], // A Sequence of YaspSource
  processes: Seq[YaspProcess], // A Sequence of YaspProcess
  sinks: Seq[YaspSink] // A Sequence of YaspSink
)

case class YaspExecution(
  session: Session, // A SessionConf instance
  plan: YaspPlan // A YaspPlan Instance
)

trait YaspService {
  // Generate the SparkSession and execute the YaspPlan
  def run(yaspExecution: YaspExecution)
}
```

#### YaspSource, YaspProcess and YaspSink

A `YaspSource` define a data source. Each `YaspSource` are loaded via a `YaspLoader` that read repartition and cache the
data to a specific `CacheLayer` and register it as a temporary table with the id provided.

A `YaspProcess` define a data process operation. Each `YaspProcess` are executed via a `YaspProcessor` that execute
the `Process`, repartition and cache the data to a specific `CacheLayer` and register it as a temporary table with the
id provided.

A `YaspSink` define a data output operation. Each `YaspSink` are executed via a `YaspWriter` that retrieve the specific
data using the provided `id` and write them to the specified destination.

```scala
case class YaspSource(
  id: String, // Unique ID to internally identify the resulting dataframe
  source: Source, // Source Sum Type
  partitions: Option[Int], // Optional number of partitions 
  cache: Option[CacheLayer] // Optional cache layer that will be used to cache resulting dataframe  
)

case class YaspProcess(
  id: String, // Unique ID to internally identify the resulting dataframe
  process: Process, // Source Sum Type
  partitions: Option[Int], // Optional number of partitions
  cache: Option[CacheLayer] // Optional cache layer that will be used to cache resulting dataframe
)

case class YaspSink(
  id: String, // Unique ID of the dataframe that should be write out.
  dest: Dest // Dest Sum Type
)

trait YaspLoader {
  //Load the Source, Cache the resulting dataframe if CacheLayer is specified and register the dataframe as temporary view
  def load(source: YaspSource): Unit
}

trait YaspProcessor {
  //Execute the Process, Cache the resulting dataframe if CacheLayer is specified and register the dataframe as temporary view
  def process(process: YaspProcess)
}

trait YaspWriter {
  // Retrieve the dataframe and write out as Dest configuration describe.
  def write(yaspSink: YaspSink): Unit
}
```

#### YaspService in action

Suppose that you have 3 data source that you should read join and then extract a simple number of users by cities.

```scala
object MyUsersByCitiesReport {

  def main(args: Array[String]): Unit = {
    YaspService().run(
      YaspExecution(
        session = Session(Local, "my-app-name", Map.empty),
        plan = YaspPlan(
          sources = Seq(
            YaspSource("users", Csv(path = "users/", Some(Map("header" -> "true"))), partitions=None,cache = None),
            YaspSource("addresses", Json(path = "addresses/", None),  partitions=None, cache = None),
            YaspSource("cities", Parquet(path = "cities/", mergeSchema = false),  partitions=None, cache = None)
          ),
          processes = Seq(
            YaspProcess("users_addresses", Sql("SELECT u.*,a.address,a.city_id FROM users u JOIN addresses a ON u.address_id=a.id"),partitions=None,cache= None),
            YaspProcess("users_addresses_cities", Sql("SELECT u.*,a.address,a.city_id FROM users_addresses u JOIN cities c ON u.city_id=c.id"), partitions=None,cache= None),
            YaspProcess("users_by_city", Sql("SELECT city,count(*) FROM users_addresses_cities GROUP BY city"), partitions=None,cache= None)
          ),
          sinks = Seq(
            YaspSink("users_by_city", Dest.Parquet(s"user-by-city",partitionBy(Seq("city"))))
          )
        )
      )
    )
  }
}

```

`Source`, `Process`, `Dest` and `CacheLayer` are all part of the `YaspCore` module. Take a look at the yasp core module
section for more details about available source, process and dest.

### YaspCore

The YaspCore module is a wrapper of all the Apache Spark primitives. Containing definition of Sources, Process, Dest,
Reader, Processor, Writer and Cache.

#### Source

There are a lot of `Source` that you can use with Yasp. Each `Source` define how Yasp will use spark to read it via the
specific `Reader`

##### Csv
```scala
case class Csv(
  path:String, 
  options:Option[Map[String,String]]
) extends Source
```

In the options field you can specify any spark csv options. **In addition to the standard spark options you can specify
a user-defined `schema`**

Examples:
```scala
//Define a basic csv
Csv(path="my-csv-path",None)

//Define a csv with header
Csv(path="my-csv-path",Some(Map("header"->"true")))

//Define a csv with custom separator
Csv(path="my-csv-path",Some(Map("sep"->";")))

//Define a csv with custom separator and user defined schema
Csv(path="my-csv-path",Some(Map("sep"->"\t","schema"->"field1 INT, field2 STRING")))
```

##### Json
```scala
case class Json(
  path:String, 
  options:Option[Map[String,String]]
) extends Source
```

In the options field you can specify any spark csv options.  **In addition to the standard spark options you can specify
the `schema` of the source.**

Examples:
```scala
//Define a basic json
Json(path="my-csv-path",None)

//Define a json with primitives as string
Json(path="my-csv-path",Some(Map("primitivesAsString"->"true")))

//Define a json with a user provided schema
Json(path="my-csv-path",Some(Map("sep"->"\t","schema"->"field1 INT, field2 STRING")))
```

##### Parquet

```scala
case class Parquet(
  path: String,
  mergeSchema: Boolean
) extends Source
```

Examples:
```scala
//Define a basic parquet
Parquet(path="my-csv-path",false)

//Define a basic parquet source with merge schema
Parquet(path="my-csv-path",true)
```

##### Orc

```scala
case class Orc(
  path: String
) extends Source
```

##### Avro
```scala
case class Avro(
  path: String,
  options: Option[Map[String, String]]
) extends Source
```

##### Xml

```scala
case class Xml(
  path: String,
  options: Option[Map[String, String]]
) extends Source
```

##### Jdbc

```scala
case class Jdbc(
  url: String,
  credentials: Option[BasicCredentials],
  options: Option[Map[String, String]]
) extends Source
```


#### Process
Define a data operations


##### Sql
```scala
case class Sql(
  query: String
) extends Process
```

#### Dest

Define a destination

##### Parquet

```scala
case class Parquet(
  path: String, // path of the destination
  partitionBy: Option[Seq[String]] // Optional Seq of column name to use for partition
) extends Dest
```

## Roadmap

* Add a Changelog
* Add BuiltIn Processor
* Add Table format as Source
* Add Table format as Dest

## Contributing

Contributions make the open source community amazing so any kind of **Contribution are greatly appreciated**.

You can contribute in three possible way:

* **Reporting a bug**: Open an issue with the tag `bug` and provide error details, expected behaviour and all possible
  information to replicate it.
* **New proposal**: Open an issue with the tag `enhancement` and provide all details regarding your suggestion.
* **Open a pull request**:
  * Fork the project
  * Create your feature branch `git checkout -b feature/my-awesome-feature`
  * Add your changes and commit `git commit -m 'my awesome feature'`
  * Push to the branch `git push origin feature/my-awesome-feature`
  * Open a pull request
  * Wait for project owners to approve or start a conversation on your pull request

### Contribution code of conduct

* Avoid massive refactors
* Provide the relative test cases for your feature/bug-fix