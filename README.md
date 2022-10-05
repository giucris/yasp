# Yasp

[![ci](https://github.com/giucris/yasp/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/giucris/yasp/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/giucris/yasp/branch/main/graph/badge.svg?token=H3T07AGY1S)](https://codecov.io/gh/giucris/yasp)

Yet Another SPark Framework

An easy and lightweight tool for data engineering process built on top of Apache Spark for ETL/ELT process.

## Introduction

Yasp was created to help data engineers working with Apache Spark to reduce their pipeline development time by using a
no-code/less-code approach.

With Yasp you can configure an ETL/ELT job that fetch data from a multiple source execute multiple transformation and
write in multiple destination

It is written in **Scala (2.12.15)** on top of **Apache Spark (3.3.0)** and managed as an **SBT (1.4.9)** multi module
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

Yasp provide 3 different module, you can use any one of them.

* **YaspApp** provide the highest possible level of abstraction for your ETL job and come with an executable main class.
  Allow you to manage complex big data etl job with a simple yml file
* **YaspService** provide all the yasp ops for your ETL job.
* **YaspCore** provide spark primitives useful for yasp operations.

### YaspApp

YaspApp module provide the highest possible level of abstraction for your ETL. You should just provide a yml definition
of your data operations, it will initialize the SparkSession and execute all the steps provided on the yml file.

YaspApp are able to interpolate environment variable into your yaml file, helping you to avoid writing secrets in your
task. Just add some placeholders like this `${my-pwd}` and yasp will interpolate it.

Currently, there are two different mode to run a YaspApp **but only one is stable, use it as a dependency on your code**

**I'm working to make the executable version stable in order to run the binary jar with the yml provided as external
file.**

#### YaspApp as library

Add the yasp-app reference to your dependencies into the `build.sbt`or `pom.xml` build and start using it in your code.

##### YaspApp in action

```scala
object MyUsersByCitiesReport {

  def main(args: Array[String]): Unit = {
    YaspApp.fromYaml(
      s"""
         |session:
         |  kind: Local
         |  name: example-app
         |  conf: {}
         |plan:
         |  sources:
         |    - id: users
         |      source:
         |        format: csv
         |          options: 
         |            path: users.csv
         |    - id: addresses
         |      source:
         |      format: json
         |        options: 
         |          path: addresses.jsonl
         |    - id: cities
         |      source:
         |      format: jdbc
         |        options: 
         |          url: my-db-url
         |          user: ${db_username}
         |          password: ${db_password}
         |          dbTable: my-table
         |  processes:
         |    - id: user_with_address
         |      process:
         |        query: >-
         |          SELECT u.name,u.surname,a.address,a.city,a.country 
         |          FROM users u 
         |          JOIN addresses a ON u.id = a.user_id
         |          JOIN cities c ON a.city_id = c.id
         |  sinks:
         |    - id: user_with_address
         |      dest:
         |        format: parquet
         |        options: 
         |          path: user_with_address
         |        partitionBy: 
         |          - country_id
         |""".stripMargin
    )
  }
}
```

The YaspApp will interpolate the yml content provided with environment variable, parse the yml into a YaspExecution and
execute it via a YaspService.

Take a look at the YaspService and YaspCore modules section for more detail on how it works.

### YaspService

You can use YaspService just as a library. Add the yasp-service reference to your dependencies into your `build.sbt`
or `pom.xml` file and then start using it.

#### YaspExecution and YaspPlan

The main component of the YaspService module are `YaspExecution`, `YaspPlan` and of course the `YaspService`.

A YaspExecution is a model that define an e2e ETL job executed by the `YaspService`.

A YaspExecution define a `Session` that will be used to create the `SparkSession` and a `YaspPlan` that describe all
data operations within the job.

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
            YaspSource("users", Source.Format("csv",options=Map("path"-> "users/","header" -> "true")), partitions = None, cache = None),
            YaspSource("addresses", Source.Format("json",options=Map("path"->"addresses/")), partitions = None, cache = None),
            YaspSource("cities", Source.Format("parquet",options=Map("path"->"cities/", "mergeSchema" ->"false")), partitions = None, cache = None)
          ),
          processes = Seq(
            YaspProcess("users_addresses", Sql("SELECT u.*,a.address,a.city_id FROM users u JOIN addresses a ON u.address_id=a.id"), partitions = None, cache = None),
            YaspProcess("users_addresses_cities", Sql("SELECT u.*,a.address,a.city_id FROM users_addresses u JOIN cities c ON u.city_id=c.id"), partitions = None, cache = None),
            YaspProcess("users_by_city", Sql("SELECT city,count(*) FROM users_addresses_cities GROUP BY city"), partitions = None, cache = None)
          ),
          sinks = Seq(
            YaspSink("users_by_city", Dest.Format("parquet",Map("path"->s"user-by-city"), partitionBy=Seq("city")))
          )
        )
      )
    )
  }
}

```

`Source`, `Process`, `Dest` and `CacheLayer` are all part of the `YaspCore` module. Take a look at the yasp core module
section for more details about the available sources, processes and destination.

### YaspCore

The YaspCore module is a wrapper of all the Apache Spark primitives. Containing definition of Sources, Process, Dest,
Reader, Processor, Writer and Cache.

#### Source

Currently Yasp Core support only Format source.

```scala
final case class Format(
      format: String,
      schema: Option[String] = None,
      options: Map[String, String] = Map.empty
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

Currently Yasp support only the Format destination.

##### Format

```scala
final case class Format(
    format:String,
    options:Map[String,String],
    mode:Option[String] = None,
    partitionBy: Seq[String] = Seq.empty
) extends Dest
```

## Roadmap

* Add a Changelog
* Add BuiltIn Processor
* Add Table format like Deltalake, Iceberg and Hudi as Source
* Add Table format like Deltalake, Iceberg and Hudi Dest
* Add data quality process
* Add collection of metrics with SparkListener
* Add Structured Streaming Execution

### Code of conduct

* Respect the code base and all the project style
* Avoid massive refactors
* Avoid meaningless class names, functions and variables
* Avoid write more than 5/10 lines of logic within a function, often due to poor single responsibility principles
* Avoid duplicated lines of codes
* Provide the relative test cases for your feature/bug-fix

## Contact

- Giuseppe
  Cristiano [Twitter](https://twitter.com/giucristiano89) [Linkedin](https://www.linkedin.com/in/giuseppe-cristiano-developer/)

## Acknowledgements

Yasp was originally created just for fun, to avoid repeating my self, and to help data engineers (I am one of them)
working with Apache Spark to reduce their pipeline development time.

I rewrote it from scratch and make it available to the open source community.