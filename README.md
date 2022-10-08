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

* clone yasp: `git clone https://github.com/giucris/yasp.git`
* yasp style test: `sbt scalafmtSbtCheck scalafmtCheckAll`  
* yasp code test: `sbt clean test`
* yasp build: `sbt assembly`
* all in one: `bash ci.sh`

## Installation

There are no dependencies in order to proceed and execute yasp. 
You have just to build it.

**I am working to provide a first release that you can directly download**

### Build

* As described on the Local Execute section, clone the repo and build yasp.
* Go to `yasp-app/target/scala2.12/` folder and you can find the executable jar file.

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

- Giuseppe Cristiano [Twitter](https://twitter.com/giucristiano89) [Linkedin](https://www.linkedin.com/in/giuseppe-cristiano-developer/)

## Acknowledgements

Yasp was originally created just for fun, to avoid repeating my self, and to help data engineers (I am one of them)
working with Apache Spark to reduce their pipeline development time.

I rewrote it from scratch and make it available to the open source community.