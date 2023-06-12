#!/bin/bash

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "office-activity" \
--property print.key=true \
--property key.separator=: