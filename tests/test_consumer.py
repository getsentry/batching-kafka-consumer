import calendar
from datetime import datetime, timedelta
from mock import patch
import time
from six.moves import range

from base import (
    FakeBatchingKafkaConsumer, FakeWorker, FakeKafkaMessage, FakeKafkaProducer
)

from batching_kafka_consumer import AbstractBatchWorker



class TestConsumer(object):
    def test_batch_size(self):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=2,
            max_batch_time=100,
            bootstrap_servers=None,
            group_id='group',
            commit_log_topic='commits',
            producer=FakeKafkaProducer(),
        )

        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()
        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3]
        assert consumer.worker.flushed == [[1, 2]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(2 + 1).encode('utf-8')  # offsets are last processed message offset + 1

    @patch('time.time')
    def test_batch_time(self, mock_time):
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            bootstrap_servers=None,
            group_id='group',
            commit_log_topic='commits',
            producer=FakeKafkaProducer(),
        )

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 0).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [1, 2, 3]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 1).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [4, 5, 6]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        mock_time.return_value = time.mktime(datetime(2018, 1, 1, 0, 0, 5).timetuple())
        consumer.consumer.items = [FakeKafkaMessage('topic', 0, i, i) for i in [7, 8, 9]]
        for x in range(len(consumer.consumer.items)):
            consumer._run_once()

        consumer._shutdown()

        assert consumer.worker.processed == [1, 2, 3, 4, 5, 6, 7, 8, 9]
        assert consumer.worker.flushed == [[1, 2, 3, 4, 5, 6]]
        assert consumer.worker.shutdown_calls == 1
        assert consumer.consumer.commit_calls == 1
        assert consumer.consumer.close_calls == 1

        assert len(consumer.producer.messages) == 1
        commit_message = consumer.producer.messages[0]
        assert commit_message.topic() == 'commits'
        assert commit_message.key() == '{}:{}:{}'.format('topic', 0, 'group').encode('utf-8')
        assert commit_message.value() == '{}'.format(6 + 1).encode('utf-8')  # offsets are last processed message offset + 1

    def test_dead_letter_topic(self):
        class FailingFakeWorker(FakeWorker):
            def process_message(*args, **kwargs):
                1 / 0

        producer = FakeKafkaProducer()
        consumer = FakeBatchingKafkaConsumer(
            'topic',
            worker=FailingFakeWorker(),
            max_batch_size=100,
            max_batch_time=2000,
            bootstrap_servers=None,
            group_id='group',
            producer=producer,
            dead_letter_topic='dlt'
        )

        message = FakeKafkaMessage('topic', partition=1, offset=2, key='key', value='value')
        consumer.consumer.items = [message]
        consumer._run_once()

        assert len(producer.messages) == 1
        produced_message = producer.messages[0]

        assert ('dlt', message.key(), message.value()) \
            == (produced_message.topic(), produced_message.key(), produced_message.value())

        assert produced_message.headers() == {'partition': '1', 'offset': '2', 'topic': 'topic'}
