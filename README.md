# batching-kafka-consumer

Code extracted from [Snuba](https://github.com/getsentry/snuba) so it could be re-used in Sentry. For now the best bet for documentation is the docstrings in [__init__.py](https://github.com/getsentry/batching-kafka-consumer/blob/master/batching_kafka_consumer/__init__.py).

In short, the `BatchingKafkaConsumer` is an abstraction that uses inversion of control (for better or worse) to provide a simple API for 1. processing events locally and then 2. flushing them (to whatever you want) as a batch. It flushes based on the number of events processed or amount of time that has passed since the last flush.
