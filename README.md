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
project.

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

**I am working to provide a first release that you can directly download**

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

Take a look to the detailed user documentation [Session](/docs/Session.md), [YaspExecution](/docs/YaspExecution.md), [YaspPlan](/docs/YaspPlan.md)

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



## Installation

## Local Usage

Yasp comes with a bundled spark so you can directly execute the jar package, that's it.

**I am working to provide a lightweight version without all the required library**

* Create some file/local db that will works as a source
* Create a yasp.yaml file that define your ETL/EL flow.
* Then run `java -jar yasp-app-*.jar --file <PATH_TO_YASP_YAML_FILE>`

### Example 1

Crate source data as CSV File:

```
id,field1,field2
1,b,c
2,x,y
```

A Yaml file:

```yaml
session:
  kind: local
  name: example-app
plan:
  sources:
    - id: my_csv
      source:
        format: csv
        schema: id INT, field1 STRING, field2 STRING
          options: 
            header: 'true'
            path: path/to/input/csv/
  processes:
    - id: my_csv_filtered
      process:
        query: SELECT * FROM my_csv WHERE id=1
  sinks:
    - id: my_csv_filtered
      dest:
        format: csv
        options: 
          header: 'true'
          path: path/to/out/csv/
```

**Test your yasp.yml**
yasp provide an input args that could be used to test your configuration file.

```bash
java -jar yasp-app-*.jar --file yasp.yaml --dry-run
```

The dry-run does not execute spark action, it just provide to you the YaspPlan that will be executed.

**Run yasp**
If the dry-run works fine, to run your flow you have just to execute something like this

```bash
java -jar yasp-app-*.jar --file yasp.yaml
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