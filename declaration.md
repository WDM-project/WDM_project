
## Contribution:

### Zihao Xu (5035783):

I initially implemented the first version of the database, which was serialized. To enhance its performance, I introduced 
support for Redis transaction pipeline. I experimented with the DTM library, which supports certain Redis actions but is 
written in Go. Since its return types differed significantly from this project and modifying them would require altering 
the DTM library source code, I decided not to proceed with it. Then, I proceeded to implement the entire Kafka flow. 
Additionally, I considered and implemented comprehensive multi-threading techniques to further improve performance.

### Alex He (5773911):

- Implemented Redis operations and basic transaction logic. 
- Helped with the deployment on k8s and stress test.
- Investigated different realizations of transactional protocols.


### Weiting Cai (5678269): 

- Worked on k8s deployment, locust stress test and deployment.
- Worked with Kafka workflow, transactional protocol, optimization (tried to use the same hashed partition but failed).
- Studied and Worked on RabbitMQ message system (although the final implementation didn't use it)
- Helped with debugging the services and deployment scripts.
- Worked with presentation.

### YuChuan Fu (5757673):

- Worked on the deployment of stress test environment, kafka, and consumers on kubernetes.
- Studied topics related to kafka and how to implement in our structure.
- Identified data type errors in business logic processing.
- Drew and participated in the design of kafka producer consumer diagram.

### Jiacheng Zhang (5744814):

- Worked with Kafka workflow, transactional protocol, multithread functions. 
- Worked on correcting the logic during checkout process.
- Studied and Implemented on dtm SAGA framework and RabbitMQ message system (although the final implementation didn't use them)
- Test different versions of application locally and help find out bugs.
- Wrote the report.