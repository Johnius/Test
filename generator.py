import json
import boto3
from datetime import timedelta, datetime
from time import sleep
from random import randint

session = boto3.Session(profile_name='prod', region_name='us-east-1')
kinesis = session.client('kinesis')

STREAM_NAME = 'test-stream-2'


class Generator:
    def run(self):
        while True:
            wait = self.generate()
            sleep(wait)

    def generate(self):
        wait = randint(50, 300) / 1000
        start = datetime.now()
        record, count = self.put_record(self.items_count)
        took = datetime.now() - start
        self.log_record(record, count, wait, took)
        return wait

    @staticmethod
    def put_record(count):
        return kinesis.put_record(StreamName=STREAM_NAME,
                                  Data=json.dumps({
                                      'added_items': count,
                                  }).encode(),
                                  PartitionKey='added_items_count'), \
            count

    @staticmethod
    def log_record(record, count, wait, took):
        print('Just sent record with seqnum %s with count %s. Waiting %s. '
              'Request took %s.' %
              (record.get('SequenceNumber'), count, timedelta(seconds=wait),
               took))

    @property
    def items_count(self):
        return randint(9, 108)


if __name__ == "__main__":
    gen = Generator()
    gen.run()
