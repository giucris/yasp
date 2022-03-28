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
describe all data operations within an ETL job as a List of `YaspSource`, a List of `YaspProcess` and a List of `YaspSink`.

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
  def run(yaspExecution: YaspExecution)
}
```

