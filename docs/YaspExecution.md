# YaspExecution

A `YaspExecution` is one of the main components of the `yasp-service` module along with `YaspPlan` and, of
course `YaspService`.

Currently is defined as follow:

```scala
case class YaspExecution(
    session: Session,   // A Session instance
    plan: YaspPlan      // A YaspPlan Instance
)
```

* **session** [**REQUIRED**]: A Session instance
* **plan** [**REQUIRED**]: A YaspPlan instance


Take a look to the detailed user documentation for [Session](/docs/Session.md) and [YaspPlan](/docs/YaspPlan.md)
