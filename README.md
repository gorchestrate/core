### Gorchestrate-Core
Gorchestrate-Core **manages and stores workflows** using Go's CSP model. 

## Intro
The main idea of this service is to **not let business people model workflows**. They suck at it. Especially when they use diagrams to do it. Business people should think big and leave all the details to programmers and users. They should not micro-manage processes by drawing pointless diagrams that will never work.

I like the idea of workflow engines - they could make workflow implementation easier. But since their creators want to earn money - most of the workflow engines are business-oriented, rather than programmer-oriented.

What I want is a workflow system created **by programmers for programmers**. One that I could use myself to model workflows and won't have to spend a week setting up 9 node cluster with unclear instructions.

Just look at example repo: https://github.com/gorchestrate/pizzaapp

Or create your own workflow using Gorchestrate SDK for Go https://github.com/gorchestrate/async

### Features

#### Linearized consistency
All updates to the workflows are done with linearized consistency, i.e. applied one after another.
If one process is receiving message on 2 channels - he will always get only 1 message. And only 1 message will be sent.

#### High Performance
You can expect >10k req/sec thoughput on a 4xCPU machine with regionally-replicated SSD storage.
    
Typical latencies are ~15ms. If one workflow is sending on channel "1" - the other workflow listening on that channel will receive message within ~30ms. Since all writes to SSD are batched - increasing throughput from 100 req/sec to 200 req/sec will not decrease latency.

#### Correctness
All operations on workflows are executed atomically. Workflows are processed with **Exactly-once** semantics.
Only one instance of workflow can be processed by client in a single point of time.


#### Integration
Gorchestrate is language-agnostic - you can define your workflows in any language/tools you want. Gorchestrate manages communication - you manage workflow logic.


### Details

Gorchestrate-Core runs on embedded RocksDB database and stores following entities:
* workflows  - current state of workflows that are managed on the server
* channels   - channels and their buffers that are used to communicate between workflows
* types      - type definitions of data sent to channels and new workflows.
* APIs       - Workflow definitions that allow us to start worklows using name & input data
* events     - events keep recovers of every change to workflow state


Each workflow can have multiple *threads* - i.e. blocking conditions for the process. 
Blocking conditions can be:
* **select** condition that waits for it's cases. Supported cases are send, recv, default, <-time.After()
* **call** condition that starts another workflow and waits for it's completion.
* **no** conditions means workflow can be only unblocked by manually updating workflow to a new state via *UpdateWorkflow()*.


General approach of working with it is the following:
1. Register Types used by your service using *PutType()*
2. Register workflow APIs that your service will be running using *PutAPI()*
3. Listen for workflow updates using *ListenWorkflowsUpdates()*
4. When you receive workflow update - process it in a separate thread and call *UpdateWorkflow()* after processing has finished

If you want to update process manually:
1. Find you workflow using *GetWorkflow()*
2. Check that it's ok and then call *LockWorkflow()*
3. Check it again and update workflow state
4. Call *UpdateWorkflow()*

If you want to start new worflow - call *NewWorkflow()*

Refer to **types.proto** for API docs.



![Architecture](https://storage.googleapis.com/artem_and_co/slct%20architecture2.png)