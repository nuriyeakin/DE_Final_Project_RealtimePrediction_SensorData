#!/bin/bash

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic "office-no-activity" \
--property print.key=true \
--property key.separator=: