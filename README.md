# sarama-cg
[WIP] consumer group for Kafka using Sarama

# why

I need to be able to provide custom topic-partition assignment protocols e.g. hashring assignment instead of round-robin to minimize partition ownership changes upon group membership change.

I need to be able to customize how I read a partition from a last-known offset e.g. when handling a new partition, rewind 2 hours of history to build context up until the last-offset.

https://cwiki.apache.org/confluence/display/KAFKA/Kafka+Client-side+Assignment+Proposal

## License

Apache License 2.0, see [LICENSE](LICENSE).
