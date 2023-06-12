#!/bin/bash

#START KAFKA
sudo systemctl start zookeeper
sudo systemctl start kafka

## Kafka Topic

python /home/train/dataops7/finalProject/kafka/admin_client.py

#data-generator env

source ~/data-generator/datagen/bin/activate

python ~/data-generator/dataframe_to_kafka.py -t 'office-input' \
-i ~/project_sensors_output3/ \
-e 'parquet' -ks ',' -rst 1 -exc 'pir'

# sudo systemctl stop zookeeper
# sudo systemctl stop kafka