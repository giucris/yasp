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


