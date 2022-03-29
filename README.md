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

* **YaspCore** provide some spark primitives usefull for yasp process.
* **YaspService** provide an high level of abraction for your ETL job.
* **YaspApp** provide an executable binary to manage your complex etl job with a simple yml file

### YaspService

You can use YaspService just as a library. Add the yasp-service reference to your dependencies into your `build.sbt`
or `pom.xml` file and then start using it.

#### YaspExecution and YaspPlan

The main component of the YaspService module are `YaspExecution` and `YaspPlan` and of course the `YaspService`.

A YaspExecution is a model that define an e2e ETL job executed by the `YaspService`.

A YaspExecution define a `SessionConf` that describe how the `SparkSession` will be created and a `YaspPlan` that
describe all data operations within an ETL job as a List of `YaspSource`, a List of `YaspProcess` and a List
of `YaspSink`.

```scala
case class SessionConf(
  sessionType: SessionType, // A SumType with two possible value Local (for local session) Distributed (for cluster session)
  appName: String, // Spark application name
  config: Map[String, String] // Spark session configuration
)

case class YaspPlan(
  sources: Seq[YaspSource], // A Sequence of YaspSource
  processes: Seq[YaspProcess], // A Sequence of YaspProcess
  sinks: Seq[YaspSink] // A Sequence of YaspSink
)

case class YaspExecution(
  conf: SessionConf, // A SessionConf instance
  plan: YaspPlan // A YaspPlan Instance
)

trait YaspService {
  // Generate the SparkSession and execute the YaspPlan
  def run(yaspExecution: YaspExecution)
}
```

#### YaspSource, YaspProcess and YaspSink

A `YaspSource` define a data source as a model with a unique `id`, a `Source` configuration and an optional `CacheLayer`
. Each `YaspSource` are loaded by the `YaspLoader` that basically read the data, cache the dataframe to the
specific `CacheLayer` and create a temporary table with the unique id provided.

A `YaspProcess` define a data process operation as a model with a unique `id`, a `Process` configuration and an
optional `CacheLayer`. Each `YaspProcess` are executed by the `YaspProcessor` that basically execute the `Process`,
cache the resulting dataframe to the specific `CacheLayer` and create a temporary table with the unique id provided.

A `YaspSink` define a data output operation as a model with a unique `id` (id of the YaspEntity that you want to write
out), and a `Dest` configuration. Each `YaspSink` are executed by the `YaspWriter` that basically will retrieve the
specific dataframe with the provided unique `id` and write them as `Dest` configuration

```scala
case class YaspSource(
  id: String, // Unique ID to internally identify the resulting dataframe
  source: Source, // Source Sum Type
  cache: Option[CacheLayer] // Optional cache layer that will be used to cache resulting dataframe
)

case class YaspProcess(
  id: String, // Unique ID to internally identify the resulting dataframe
  process: Process, // Source Sum Type
  cache: Option[CacheLayer]  // Optional cache layer that will be used to cache resulting dataframe
)

case class YaspSink(
  id: String,  // Unique ID of the dataframe that should be write out.
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
object MyUsersByCitiesReport{
  
  def main(args: Array[String]): Unit ={
    YaspService().run(
      YaspExecution(
        conf = SessionConf(Local, "my-app-name", Map.empty),
        plan = YaspPlan(
          sources = Seq(
            YaspSource("users", Source.Csv(path = "users/",Some(Map("header"->"true"))),cache = None),
            YaspSource("addresses", Source.Json(path = "addresses/",None),cache = None),
            YaspSource("cities", Source.Parquet(path = "cities/",mergeSchema=false),cache = None)
          ),
          processes = Seq(
            YaspProcess("users_addresses", Sql("SELECT u.*,a.address,a.city_id FROM users u JOIN addresses a ON u.address_id=a.id"), None),
            YaspProcess("users_addresses_cities", Sql("SELECT u.*,a.address,a.city_id FROM users_addresses u JOIN cities c ON u.city_id=c.id"), None),
            YaspProcess("users_by_city", Sql("SELECT city,count(*) FROM users_addresses_cities GROUP BY city"), None)
          ),
          sinks = Seq(
            YaspSink("users_by_city", Dest.Parquet(s"user-by-city"))
          )
        )
      )
    )
  }
}

```

That's it. just 20 line of code that is just configuration code.


`Source`, `Process`, `Dest` and `CacheLayer` are all part of the `YaspCore` module. Take a look at the yasp core module 
chapter on the README.md for more details about available source, process and dest.