from kafka import KafkaProducer, KafkaConsumer, KafkaAdminClient
from kafka.admin import NewTopic
import json

class KafkaManager:
    def __init__(self, bootstrap_servers="localhost:9092"):
        self.admin_client = KafkaAdminClient(bootstrap_servers=bootstrap_servers)
        self.producer = KafkaProducer(bootstrap_servers=bootstrap_servers,
                                      value_serializer = lambda v:json.dumps(v).encode('utf-8'))
        self.consumer = KafkaConsumer(bootstrap_servers=bootstrap_servers,
                                      auto_offset_reset='earliest',
                                      group_id='my_group',
                                      value_deserializer=lambda x: json.loads(x.decode('utf-8')))
    
    def create_topic(self, topic_name, num_partitions=1, replication_factor=1):
        topic_list = []
        new_topic = NewTopic(name=topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
        topic_list.append(new_topic)
        self.admin_client.create_topics(new_topics=topic_list, validate_only=False)
    
    def send_message(self, topic, message):
        self.producer.send(topic, value=message)
        self.producer.flush()

    def subscribe(self, topics):
        self.consumer.subscribe(topics)

    def consume_messages(self):
        for message in self.consumer:
            yield message.value

    def close(self):
        self.admin_client.close()
        self.producer.close()
        self.consumer.close()

# Example Usage
manager = KafkaManager()

# Creating a topic
manager.create_topic('bankbranch', num_partitions=2, replication_factor=1)

# Sending a message
manager.send_message('bankbranch', {'key': 'value'})

# Consuming messages
manager.subscribe(['bankbranch'])
for message in manager.consume_messages():
    print("Received:", message)

# Closing connections
manager.close()

