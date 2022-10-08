# YaspService

A YaspService is the main component of the `yasp-service` module.

It contains all the logic to execute a `YaspExecution`

Currently is defined as follow
```scala
trait YaspService {
  // Generate the SparkSession and execute the YaspPlan
  def run(yaspExecution: YaspExecution)
}
```


