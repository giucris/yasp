# Yasp [![ci](https://github.com/giucris/yasp/actions/workflows/ci.yml/badge.svg?branch=develop)](https://github.com/giucris/yasp/actions/workflows/ci.yml) [![codecov](https://codecov.io/gh/giucris/yasp/branch/main/graph/badge.svg)](https://codecov.io/gh/giucris/yasp)

[![SonarCloud](https://sonarcloud.io/images/project_badges/sonarcloud-black.svg)](https://sonarcloud.io/summary/new_code?id=giucris_yasp)

[![Lines of Code](https://sonarcloud.io/api/project_badges/measure?project=giucris_yasp&metric=ncloc)](https://sonarcloud.io/summary/new_code?id=giucris_yasp) [![Security Rating](https://sonarcloud.io/api/project_badges/measure?project=giucris_yasp&metric=security_rating)](https://sonarcloud.io/summary/new_code?id=giucris_yasp) [![Maintainability Rating](https://sonarcloud.io/api/project_badges/measure?project=giucris_yasp&metric=sqale_rating)](https://sonarcloud.io/summary/new_code?id=giucris_yasp) [![Reliability Rating](https://sonarcloud.io/api/project_badges/measure?project=giucris_yasp&metric=reliability_rating)](https://sonarcloud.io/summary/new_code?id=giucris_yasp)

Yet Another SPark Framework

An easy and lightweight tool for data engineering process built on top of Apache Spark for ETL/ELT process.

## Introduction

Yasp was created to help data engineers working with Apache Spark to reduce their pipeline development time by using a
**no-code/less-code** approach.

With Yasp you can configure an ETL/ELT job that fetch data from a multiple source execute multiple transformation and
write in multiple destination

It is written in **Scala (2.12.15)** on top of **Apache Spark (3.3.0)** and managed as an **SBT (1.4.9)** multi module
project comes with the following modules: 
* **YaspApp** provide the highest possible level of abstraction for your ETL job and come with an executable main class.
  Allow you to manage complex big data etl job with a simple yml file
* **YaspService** provide all the yasp ops for your ETL job.
* **YaspCore** provide spark primitives useful for yasp operations.


## Getting Started

### Prerequisites

* JDK 8 Installed
* SBT Installed
* Windows user only: This is an Apache Spark based framework to run it locally you should configure the hadoop winutils
  on your laptop. Check [here](https://github.com/steveloughran/winutils) for more details.

### Local Execute

To execute Yasp you should first build it.

* clone: `git clone https://github.com/giucris/yasp.git`
* build: `sbt assembly`
* Go to `yasp-app/target/scala2.12/` folder and you can find the executable jar file.

For devs:

* yasp style test: `sbt scalafmtSbtCheck scalafmtCheckAll`
* yasp code test: `sbt clean test`
* all in one: `bash ci.sh`

**NB: I'm working to provide a first release that you can directly download**

To execute yasp you have to provide a single yaml file (json is fine too), that basically will contain all your Extract
Transform and Load task.

#### yasp.yaml
A yasp.yaml is a file that define a `YaspExecution`.
A `YaspExecution` is a model that define an e2e ETL/EL job in terms of `Session` and `YaspPlan`. 
A `YaspPlan` is a model that define all the data operations that yasp will execute. 

For example:
```yaml
# Session field
session: 
  kind: local               #Define a local spark session. Use distributed for non local execution.
  name: example-app         #Define a Spark Appcliation name.

# YaspPlan field
plan:   
  # List of all sources
  sources:
    - id: my_csv            # Id of the source. used by yasp to register the dataset as a table
      source:               # Source fiedl, will contains all the configuration to read the data
        format: csv         # Standard Spark format
        options:            # Standard Spark format options
          header: 'true'
          path: path/to/input/csv/
  # List of all process
  processes:
    - id: my_csv_filtered   # Id of the process. used by yasp to register the resulting data as a table
      process:              # Process field, will contains all the Process configuration to transform the data
        query: >-           # A sql process configuration
          SELECT * 
          FROM my_csv 
          WHERE id=1
  # List of sinks
  sinks:                   
    - id: my_csv_filtered   # Id of the source/process registered that will be written 
      dest:                 # Destination field, contains all the Dest configuration to write the data
        format: csv         # Standard Spark format
        options:            # Standard Spark format options
          header: 'true'
          path: path/to/out/csv/
```

Take a look to the detailed user documentation for [Session](/docs/Session.md), [YaspExecution](/docs/YaspExecution.md), [YaspPlan](/docs/YaspPlan.md)

### Supported sources

Currently Yasp support all default Spark format sources plus:
* XML
* Delta table 

Take a look to the detailed user documentation for all the yasp source 

### Supported transformation

Currently Yasp support only SQL transformation

Take a look to the detailed user documentation for all the yasp processor

### Supported destination

Currently Yasp support all default Spark format destination plus: 
* XML
* Delta table

Take a look to the detailed user documentation for all the yasp destination

## Usage

Currently you can use Yasp in three different way
**NB** You have to build yasp before using it, please follow the relative section.

### Local usage

Yasp comes with a bundled spark, so you can directly execute the jar package on your machine.
**NB: Please take a look to the prerequisites needed to run it locally**

**Execute an ETL/EL**
* Create a yasp.yaml file that define your ETL/EL flow.
* Then run yasp: 
```bash
java -jar yasp-app-x.y.z.jar --file <PATH_TO_YASP_YAML_FILE>`
```

**Test your yasp.yml**
* Create a yasp.yaml file that define your ETL/EL flow.
* Then run yasp with dry-run enabled:
```bash
java -jar yasp-app-x.y.z.jar --file <PATH_TO_YASP_YAML_FILE> --dry-run
```

The dry-run does not execute spark action, it just provide to you the YaspPlan that will be executed.

### Cluster usage

* Create a yasp.yaml file and make it available for your cluster
* Then run yasp as main class for your spark submit:
```bash
  spark-submit --class it.yasp.app.Yasp yasp-app-x-y-z.jar --file yasp.yaml
```

### Library usage

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

## Roadmap

* Add BuiltIn Processor
* Add support for Iceberg and Hudi as Source/Dest
* Add data quality process
* Add Structured Streaming Execution

### Contribution and Code of conduct

See relative file for more information:

* [Contributing](CONTRIBUTING.md)
* [Code of Conduct](CODE_OF_CONDUCT.md)

## Contact

- Giuseppe
  Cristiano [Twitter](https://twitter.com/giucristiano89) [Linkedin](https://www.linkedin.com/in/giuseppe-cristiano-developer/)

## Acknowledgements

Yasp was originally created just for fun, to avoid repeating my self, and to help data engineers (I am one of them)
working with Apache Spark to reduce their pipeline development time.

I rewrote it from scratch and make it available to the open source community.