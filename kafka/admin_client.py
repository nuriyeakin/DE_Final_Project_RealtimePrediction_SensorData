from kafka.admin import KafkaAdminClient, NewTopic, ConfigResource, ConfigResourceType
import time
import os

client = KafkaAdminClient (
    bootstrap_servers = ['localhost:9092'],
    client_id='local_client'
)

try:
    topic_list = ['office-input','office-no-activity','office-activity']
    new_topics = [NewTopic(topic,num_partitions=3, replication_factor=1) for topic in topic_list]
    client.create_topics(new_topics)

except:
    print("Topic exist")

time.sleep(1)

print(client.list_topics())