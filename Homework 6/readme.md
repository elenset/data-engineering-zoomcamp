# Homework

In this homework, we're going to learn about streaming with PyFlink.

Instead of Kafka, we will use Red Panda, which is a drop-in
replacement for Kafka. It implements the same interface, 
so we can use the Kafka library for Python for communicating
with it, as well as use the Kafka connector in PyFlink.

For this homework we will be using the Taxi data:
- Green 2019-10 data from [here](https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/green_tripdata_2019-10.csv.gz)


## Setup

We need:

- Red Panda
- Flink Job Manager
- Flink Task Manager
- Postgres

It's the same setup as in the [pyflink module](../../../06-streaming/pyflink/), so go there and start docker-compose:

```bash
cd ../../../06-streaming/pyflink/
docker-compose up
```

(Add `-d` if you want to run in detached mode)

Visit http://localhost:8081 to see the Flink Job Manager

Connect to Postgres with pgcli, pg-admin, [DBeaver](https://dbeaver.io/) or any other tool.

The connection credentials are:

- Username `postgres`
- Password `postgres`
- Database `postgres`
- Host `localhost`
- Port `5432`

With pgcli, you'll need to run this to connect:

```bash
pgcli -h localhost -p 5432 -u postgres -d postgres
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

## Question 2. Creating a topic

### Answer
```bash
redpanda@123fe502e80a:/$ rpk topic create green-trips
TOPIC        STATUS
green-trips  OK
```
#### Solution
- https://docs.redpanda.com/current/reference/rpk/rpk-topic/rpk-topic-create/


## Question 3. Connecting to the Kafka server

### Answer
- True


#### Solution
- Run jupyter notebook and run the code for Q3. See detail in [Homework with Flink.ipynb](https://github.com/elenset/data-engineering-zoomcamp/blob/main/Homework%206/Homework.ipynb) file

## Question 4: Sending the Trip Data

### Answer
- It took 62.78 seconds to send the entire dataset and flush


#### Solution
- Run jupyter notebook and run the code for Q4. See detail in [Homework with Flink.ipynb](https://github.com/elenset/data-engineering-zoomcamp/blob/main/Homework%206/Homework.ipynb) file


## Question 5: Build a Sessionization Window (2 points)

### Answer
- LocationIDs are 74 and 75 (Manhattan East Harlem North & South)


#### Solution
Prepare [aggregation_job.py](https://github.com/elenset/data-engineering-zoomcamp/blob/main/Homework%206/aggregation.py) and sink table in database
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

- Form for submitting: https://courses.datatalks.club/de-zoomcamp-2025/homework/hw6
- Deadline: See the website
