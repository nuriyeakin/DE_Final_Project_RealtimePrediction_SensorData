# DE_Final_RealtimePrediction_SensorData

### 0. Start Jupyter Lab
```
[train@trainvm ]$ cd final_project
```
```
[train@trainvm final_project]$ source ~/venvspark/bin/activate
```
```
(venvspark) [train@trainvm final_project]$ jupyter lab
```
### 1. Cleaning the Data
* Continue from 01_read_and_clean_the_data.ipynb

### 2. Model train
* 02_model_train.py
- Training and saving the model

### 3. Produce with Kafka
* 03_producer.sh
- Create the topics with admin_client.py file

### 4. Consumers
* 04_consumer-activity.sh
-If there is activity data will come to this topic
* 04_consumer-no-activity.sh
- If there is no activity data will come to this topic

### 5. Spark Stream to Kafka
* 05_spark-stream-to-kafka.py
