
from kafka import KafkaProducer
import json
from json import loads
from csv import DictReader

# Required setting for Kafka Producer
bootstrap_servers = ['localhost:9092']
topicname = 'first_topic'
producer = KafkaProducer(bootstrap_servers = bootstrap_servers)
producer = KafkaProducer()


# 1.Iterate over each line in tokenized_access_logs.csv file 
# 2.Send each recoer to kafktopi
# 3.For confirmation we are printing topic name and partiion 
with open('tokenized_access_logs.csv','r') as read_obj:
    csv_dict_reader = DictReader(read_obj)
    for row in csv_dict_reader:
        ack = producer.send(topicname, json.dumps(row).encode('utf-8'))
        metadata = ack.get()
        print(metadata.topic, metadata.partition)
