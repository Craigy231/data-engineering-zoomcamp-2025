
- Course homework page: https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/cohorts/2025/06-streaming/homework.md
- Course materials: https://github.com/DataTalksClub/data-engineering-zoomcamp/tree/main/06-streaming
- Due date: 18.03.2025 02:00 (local time)



In this homework, we're going to learn about streaming with PyFlink.
Instead of Kafka, we will use Red Panda, which is a drop-in replacement for Kafka. It implements the same interface, so we can use the Kafka library for Python for communicating with it, as well as use the Kafka connector in PyFlink.

For this homework we will be using the Taxi data:
- Green 2019-10 data from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz)

## Setup
We need:
- Red Panda
- Flink Job Manager
- Flink Task Manager
- Postgres

It's the same setup as in the [pyflink module](https://github.com/DataTalksClub/data-engineering-zoomcamp/blob/main/06-streaming/pyflink), so go there and start docker-compose:
```shell
cd ../../../06-streaming/pyflink/
docker-compose up
```

(Add `-d` if you want to run in detached mode)
Visit [http://localhost:8081](http://localhost:8081/) to see the Flink Job Manager
Connect to Postgres with pgcli, pg-admin, [DBeaver](https://dbeaver.io/) or any other tool.
The connection credentials are:

- Username `postgres`
- Password `postgres`
- Database `postgres`
- Host `localhost`
- Port `5432`

Some preparation
```bash
cd ~/DEZC/Projects/week6/pyflink

mkdir data # Create and put source data into the folder

wget https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz
gzip -d green_tripdata_2019-10.csv.gz 

cd ~/DEZC/Projects/week6/pyflink
explorer.exe .
sudo docker-compose up #make up
jupyter notebook

```
Run these query to create the Postgres landing zone for the first events and windows:
```sql
CREATE TABLE processed_events (
    test_data INTEGER,
    event_timestamp TIMESTAMP
);

CREATE TABLE processed_events_aggregated (
    event_hour TIMESTAMP,
    test_data INTEGER,
    num_hits INTEGER 
);
```


## Question 1: Redpanda version
Now let's find out the version of redpandas.
For that, check the output of the command `rpk help` _inside the container_. The name of the container is `redpanda-1`.
Find out what you need to execute based on the `help` output.
What's the version, based on the output of the command you executed? (copy the entire version)

### Answer
- 24.2.18


#### Solution
[Run](https://cdn.prod.website-files.com/6659da8aecd70e0898c0d7ed/672fc8c9eb42cf5b27ceb1ba_redpanda-rpk-commands-cheat-sheet.pdf) interactive mode in docker container and get the Red Panda version 

```bash
lan@vlan:~$ docker exec -it redpanda-1 /bin/bash
redpanda@123fe502e80a:/$ rpk version
Version:     v24.2.18
Git ref:     f9a22d4430
Build date:  2025-02-14T12:52:55Z
OS/Arch:     linux/amd64
Go version:  go1.23.1

Redpanda Cluster
  node-1  v24.2.18 - f9a22d443087b824803638623d6b7492ec8221f9
```
## Question 2. Creating a topic
Before we can send data to the redpanda server, we need to create a topic. We do it also with the `rpk` command we used previously for figuring out the version of redpandas.
Read the output of `help` and based on it, create a topic with name `green-trips`
What's the output of the command for creating a topic? Include the entire output in your answer.

### Answer
```bash
redpanda@123fe502e80a:/$ rpk topic create green-trips
TOPIC        STATUS
green-trips  OK
```
#### Solution
- https://docs.redpanda.com/current/reference/rpk/rpk-topic/rpk-topic-create/



## Question 3. Connecting to the Kafka server
We need to make sure we can connect to the server, so later we can send some data to its topics
First, let's install the kafka connector (up to you if you want to have a separate virtual environment for that)
```shell
pip install kafka-python
```
You can start a jupyter notebook in your solution folder or create a script
Let's try to connect to our server:
```python
import json

from kafka import KafkaProducer

def json_serializer(data):
    return json.dumps(data).encode('utf-8')

server = 'localhost:9092'

producer = KafkaProducer(
    bootstrap_servers=[server],
    value_serializer=json_serializer
)

producer.bootstrap_connected()
```
Provided that you can connect to the server, what's the output of the last command?
### Answer
- True


#### Solution
- Run jupyter notebook and run the code for Q3. See detail in [Homework with Flink.ipynb](https://github.com/alexanderlazutkin/DEZC2025/blob/main/week6/Homework%20with%20Flink.ipynb) file

## Question 4: Sending the Trip Data
Now we need to send the data to the `green-trips` topic
Read the data, and keep only these columns:

- `'lpep_pickup_datetime',`
- `'lpep_dropoff_datetime',`
- `'PULocationID',`
- `'DOLocationID',`
- `'passenger_count',`
- `'trip_distance',`
- `'tip_amount'`

Now send all the data using this code:
```python
producer.send(topic_name, value=message)
```
For each row (`message`) in the dataset. In this case, `message` is a dictionary.
After sending all the messages, flush the data:
```python
producer.flush()
```
Use `from time import time` to see the total time
```python
from time import time
t0 = time()
# ... your code
t1 = time()
took = t1 - t0
```
How much time did it take to send the entire dataset and flush?
### Answer
- It took 62.78 seconds to send the entire dataset and flush


#### Solution
- Run jupyter notebook and run the code for Q4. See detail in [Homework with Flink.ipynb](https://github.com/alexanderlazutkin/DEZC2025/blob/main/week6/Homework%20with%20Flink.ipynb) file


## Question 5: Build a Sessionization Window (2 points)
Now we have the data in the Kafka stream. It's time to process it.
- Have it read from `green-trips` fixing the schema
- Use a [session window](https://nightlies.apache.org/flink/flink-docs-master/docs/dev/datastream/operators/windows/) with a gap of 5 minutes
- Use `lpep_dropoff_datetime` time as your watermark with a 5 second tolerance
- Which pickup and drop off locations have the longest unbroken streak of taxi trips?
### Answer
- LocationIDs are 74 and 75 (Manhattan East Harlem North & South)


#### Solution
Prepare [aggregation_job.py](https://github.com/alexanderlazutkin/DEZC2025/blob/main/week6/aggregation_job.py) and sink table in database
```sql
drop table if exists aggregated_trips;
CREATE TABLE aggregated_trips (
PULocationID INTEGER,
DOLocationID INTEGER,
session_start TIMESTAMP(3),
session_end TIMESTAMP(3),
num_trips BIGINT,
session_duration BIGINT ,
PRIMARY KEY (PULocationID, DOLocationID, session_start)
);
```

Run producer for Q4 sending trip data
```bash
cd ~/DEZC/Projects/week6/pyflink
docker compose exec jobmanager ./bin/flink run -py /opt/src/job/aggregation_job.py --pyFiles /opt/src -d
```

Getting the result in table
```sql
select * from aggregated_trips;

pulocationid	dolocationid	session_start	session_end	num_trips	session_duration
75	74	2019-10-08 17:34:02.000	2019-10-08 19:24:49.000	44	6,647

```


## Submitting the solutions

- Form for submitting: [https://courses.datatalks.club/de-zoomcamp-2025/homework/hw6](https://courses.datatalks.club/de-zoomcamp-2025/homework/hw6)
