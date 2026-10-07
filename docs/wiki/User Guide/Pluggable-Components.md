## Metric Sampler
The metric sampler is one of the most important pluggables in Kafka Cruise Control, it allows user to easily deploy Cruise Control to various environment and work with the existing metric system.

The default implementation of metric sampler is reading the broker metrics produced by `CruiseControlMetricsReporter` on the broker. This is assuming that users are running Kafka brokers by setting the `metric.reporters` configuration on the Kafka brokers to be `com.linkedin.kafka.cruisecontrol.metricsreporter.CruiseControlMetricsReporter`(see [quick start](https://github.com/linkedin/cruise-control#quick-start) on how to do that).

## Metric Sampler Partition Assignor
When users have multiple metric sampler threads, the metric sampler partition assignor is responsible for assigning partitions to the metric samplers.
This is useful when users have some existing metric system. The default implementation assigns all the partitions of the same topic to the same metric sampler.

## Sample Store
The Sample Store is used to store the collected metric samples and training samples to external storage. One problem in metric sampling is that we are using some derived data from the raw metrics. And the way we derive the data relies on the metadata of the cluster at that moment. So when we look at the old metrics, if we do not know the metadata at the point the metric was collected the derived data would be inaccurate. Sample Store helps solve this problem by storing the derived data directly to an external storage for later loading.

The default implementation of Sample Store produces the samples back to the Kafka topic.

## Broker Capacity Config Resolver
The broker Capacity Config Resolver is the way for Cruise Control to get the broker capacity for each of the resources. The default implementation is file based properties. Users can also have a customized implementation to retrieve the capacity of the brokers from some hardware resource management system.

## Goals
The goals in Kafka Cruise Control are pluggable with different priorities.

* **Rack-awareness**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner.

 * **RackAwareDistributionGoal** - Contrary to `RackAwareGoal`, as long as replicas of each partition can achieve a perfectly
 even distribution across the racks, this goal lets placement of multiple replicas of a partition into a single rack.

* **ReplicaCapacityGoal**: Attempt to make all the brokers in a cluster to have less than a given number of replicas.

* **CapacityGoals**: Goals that ensures the broker resource utilization is below a given threshold for the corresponding resource. Capacity goals are: 
    * DiskCapacityGoal
    * NetworkInboundCapacityGoal
    * NetworkOutboundCapacityGoal
    * CpuCapacityGoal

* **ReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **PotentialNwOutGoal**: A goal that ensures the potential network output (when all the replicas becomes leaders) on each of the broker do not exceed the broker’s network outbound bandwidth capacity.

* **ResourceDistributionGoals**: Attempt to make the resource utilization variance among all the brokers are within a certain range. This goal does not do anything if the cluster is in a low utilization mode (when all the resource utilization of each broker is below a configured percentage.) This is not a single goal, but consists of the following separate goals for each of the resources. 
    * DiskUtilDistributionGoal
    * NetworkInboundUtilDistributionGoal
    * NetworkOutboundUtilDistributionGoal
    * CpuUtilDistributionGoal

* **TopicReplicaDistributionGoal**: Attempt to make the replicas of the same topic evenly distributed across the entire cluster.

* **LeaderReplicaDistributionGoal**: Attempt to make all the brokers in a cluster to have the similar number of leader replicas.

* **LeaderBytesInDistributionGoal**: Attempt to make the leader bytes in rate on each host to be balanced.

* **PreferredLeaderElectionGoal**: Attempt to make the first replica in replica list leader replica of the partition for all topic partition.

* **KafkaAssignerGoals**: These goals are used to make Cruise Control behaves like [Kafka assigner tool](https://github.com/linkedin/kafka-tools/wiki/Kafka-Assigner). These goals will be picked up if `kafka_assigner` parameter is set to true in corresponding request(e.g. [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).
    * **KafkaAssignerDiskUsageDistributionGoal**: A goal that ensures all the replicas of each partition are assigned in a rack aware manner. 
    * **KafkaAssignerEvenRackAwareGoal**: Attempt to make all the brokers in a cluster to have the similar number of replicas.

* **IntraBrokerDiskCapacityGoal**: Goals that ensures the disk resource utilization is below a given threshold. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance). Not available in `kafka_0_11_and_1_0` branch. 

* **IntraBrokerDiskUsageDistributionGoal**: Attempt to make the utilization variance among all the disks within same broker are within a certain range. This goal will be pick up if `rebalance_disk` parameter is set to `true` in [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance). Not available in `kafka_0_11_and_1_0` branch. 

* **BrokerSetAwareGoal**: BrokerSet is defined as a subset of brokers in the cluster. This Goal will Attempt to make the replica movements constrained within the boundary of a BrokerSet.

## Anomaly Notifier
The anomaly notifier is a communication channel between Cruise Control and users.
It notifies users about the anomalies detected in the cluster as well as actions taken about the anomaly. Anomalies include:
* Broker failure
* Goal violation
* Metric Anomaly
* Disk failure (not available in `kafka_0_11_and_1_0` branch)
* Topic Anomaly

The actions users can take are:
* Fix the anomaly
* Wait some time and check if the anomaly still exists
* Ignore the anomaly

By default Cruise Control is configured to use `NoopNotifier` which ignores all the anomalies.

## Replica Movement Strategy
The strategy to determine the execution order for generated proposals. By default `BaseReplicaMovementStrategy` is used, which is totally random.
Sometimes this could result in prolonged execution time due to some long tail tasks in each execution batches. Other available strategies includes:
* **PrioritizeSmallReplicaMovementStrategy**: prioritize small sized replicas
* **PrioritizeLargeReplicaMovementStrategy**: prioritize large sized replicas
* **PostponeUrpReplicaMovementStrategy**: prioritize replicas for partition having no out-of-sync replica
* **PrioritizeMinIsrWithOfflineReplicasStrategy**: prioritize tasks with (At/Under)MinISR partitions with offline replicas

The strategies can be chained to use and can be dynamically set using `replica_movement_strategies` in corresponding request(e.g. [rebalance request](https://github.com/linkedin/cruise-control/wiki/REST-APIs#trigger-a-workload-balance)).

## Maintenance Event Reader
When a system/tool in Kafka Ecosystem has knowledge about an upcoming planned maintenance, such as a switch change, an upcoming VM freeze, 
or reboot of VMs on cloud deployments, the desired preventative mitigation strategy (i.e. `maintenance event`) can be conveyed to 
Cruise Control for automated execution of it without human intervention.

Such maintenance events correspond to admin operations that the relevant system/tool submits to Cruise Control via a generic maintenance event store.
Then, Maintenance Event Reader retrieves the maintenance events from the relevant event store for automated execution.

The default implementation of Maintenance Event Reader (see `MaintenanceEventTopicReader`) consumes maintenance events from a Kafka topic.

## Provisioner
This component provides the means for adding or removing resources to / from the cluster.

Resources include:
* Brokers that can host replicas 
* Racks containing brokers that can host replicas
* Disks (i.e. relevant to JBOD deployments)
* Partitions of a topic (can only be added)

Constraints include:
* Topic name: If the resource is partition, the name of the topic must be specified.
* A typical broker id and its capacity (one cannot be specified without the other)
* Specific resource, such as {@link Resource#DISK}.
* Excluded racks -- i.e. racks for which brokers should not be added to or removed from
* Total resource capacity required to add or remove

## Broker Set Resolver
The broker set resolver is the way for Cruise Control to get the broker set information for all the brokers. The default implementation is file based properties. Users can also have a customized implementation to retrieve the broker set to broker mapping from some other source.

## Default Broker Set Assignment Policy
The default broker set assignment policy handles the case where a broker id is not provided with any mapping to a broker set. There is a default NoOpDefaultBrokerSetAssignmentPolicy that can be used if there is no default mapping and any mismatch will result in fast failure. Users can have a customized implementation to provide a default treatment for unmapped brokers.

### Disk-aware inter-broker replica placement

Enable `inter.broker.disk.capacity.check.enabled=true` to check individual JBOD log directories when moving replicas between brokers. This is disabled by default, preserving existing broker-level balancing. Configure the actual absolute log-directory paths and their capacities in MB in the broker capacity resolver for every live broker, including brokers with only one log directory. Each configured log directory should correspond to a separate disk; directories sharing a filesystem do not have independent capacity.

The planner fetches replica placement and total Kafka replica bytes through `describeLogDirs`, including unmonitored and future replicas. When the broker reports filesystem total and usable bytes, it uses the larger of Kafka replica usage and filesystem usage, and caps configured capacity at the actual filesystem size. It selects an alive, positive-capacity disk with the lowest projected utilization percentage for which `usage + incoming replica size < disk capacity * disk.capacity.threshold`. Incoming size is the larger of sampled size and the largest live replica size, rounded up to MB. It reserves space for each final incoming copy and does not credit space from proposed outgoing copies. If optimization redirects or deletes a planned copy, its superseded reservation is released; the live source usage remains reserved until Kafka deletes the files. This protects concurrent copies and swaps, but can reject plans that would fit only after outgoing replicas have been deleted. Existing broker capacity, rack placement, exclusions, and other goal constraints still apply. Missing capacity, usage, or unexplained placement information fails the operation rather than falling back to broker-only placement. When a broker reports a failed disk before cluster metadata marks its replicas offline, missing placements on that broker are modeled as offline replicas so self-healing can continue. Failed disks remain ineligible destinations. A configured threshold of zero is accepted and permits no incoming copies.

Replication-factor increases also select and reserve a disk when creating future replicas. Initial rack-aware assignment skips capacity-ineligible brokers and rejects only after exhausting the eligible candidates.

Disk-aware proposals expose `destinationLogDirs` in their JSON. Before submitting each batch, the executor refreshes disk usage, checks incoming sizes against live source sizes, and reserves remaining bytes for all active copies. It then calls `alterReplicaLogDirs` and waits for each acknowledgement before submitting the broker reassignment. Kafka's `REPLICA_NOT_AVAILABLE` response is expected for a replica that has not been created yet: Kafka stores the requested directory for its later creation. Other placement errors prevent reassignment submission. Retries preserve the selected directory. A rejected inter-broker batch stops new submissions and settles previously submitted reassignments through cancellation and rollback before removing broker and topic throttles. Placement confirmation tolerates transient query failures; three consecutive failures stop execution through the same cleanup path. Completion checks batch replica-directory queries across tasks in each poll, and successful completion requires that the added replicas are on their selected disks. A replica already present on a different disk causes the operation to fail rather than starting an unplanned disk move.

This feature works with standard inter-broker and Kafka assigner goals. The capacity check remains enabled when replica placement is requested for intra-broker goals. Intra-broker moves and swaps also check live usage and reserved space during planning; their proposals carry destination capacities, and initial execution and retries revalidate space before submitting `alterReplicaLogDirs`. Intra-broker copies reserve destination space for later moves in the same optimization. `rebalance_disk=true` continues to select the separate intra-broker goals; it is not required for disk-aware inter-broker placement. Broker-only behavior remains available with the new configuration set to `false`. Kafka brokers must support `alterReplicaLogDirs` (Kafka 1.1 or later), and the Cruise Control principal needs permission to describe log directories and alter replica log directories.

When a broker does not report filesystem total and usable bytes, checks use Kafka replica bytes against configured disk capacity; that fallback does not account for other applications using the filesystem. Usage can grow after a check. Keep capacities accurate, retain disk headroom with `disk.capacity.threshold`, and generate a fresh proposal after changing disk capacities. A failure after installing some directory preferences can leave those preferences in Kafka even though the batch's broker reassignment was not submitted; partial placement acknowledgements are not transactional.

The legacy reassignment submission helper accepts broker-only starts and cancellations. Disk-aware starts must use the overload that supplies the operator-configured threshold, log-directory timeout, and active tasks; omitting these inputs is rejected.
