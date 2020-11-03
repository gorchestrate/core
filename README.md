### Gorchestrate Core
Gorchestrate Core is a database for storing asynchronous processes using Go Way.
It manages their execution, ensures linearized consistency level and has a log of events that you can subscribe to.

This service solves couple of problems, compared to simply using DB, MQ or similar products:
* Linearized consistency with > 10k req/sec. 
* Immutable log of events in the system.
* Lock management with ability for single process to have multiple threads.
* Type-safety for operations between processes with ability to update types in backward-compatible manner.

![Architecture](https://storage.googleapis.com/artem_and_co/slct%20architecture2.png)