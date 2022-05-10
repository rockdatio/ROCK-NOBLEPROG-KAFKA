**⚠️ This repository has been archived. ⚠️**

---

# Apache Flink® SQL Training

### Requirements

Use Docker Compose to setup the training environment.

You **only need [Docker](https://www.docker.com/)** to run this training. </br>
You don't need Java, Scala, or an IDE.

### Getting the Docker Compose Configuration
Docker Compose environments are configured with a YAML file. The default path of the file is docker-compose.yml.

Get the configuration file for the training environment by cloning our Git repository
```sql
git clone https://github.com/ververica/sql-training
```

----
### Starting the Training Environment
In the root of the project
```sql
docker-compose up
```

Linux & MacOS


```sql
docker-compose up
```

Windows

```sql
set COMPOSE_CONVERT_WINDOWS_PATHS=1
docker-compose up
```
(WINDOW)If there are any port in used kill it and retry composing the containers.
```sql
netstat -aon | findstr "9000"
taskkill /pid 52760
```

### Run the Flink SQL CLI Client
Run the following command to enter the Flink SQL CLI.
you should use the container with that description *apache-flink-sql-client-1*

```sql
docker ps
```
#####apache-flink-sql-client-1

```sql
docker exec -it 182a0 bash
```
Next execute then you will have entered in sql flink session.
```sql
./sql-client.sh
o 
sql-client.sh
```
Test commands
```sql
SHOW CATALOGS;
SHOW DATABASES;
SHOW TABLES;
SHOW FUNCTIONS;
DESCRIBE Rides;
```
Some tables and data are pre-registered in Docker Compose are viewed by running SHOW TABLES. By using the Rides table data in the article, you can see the driving record data stream of a taxi, including the time and location. You can view the table structure by running the DESCRIBE Rides; command
```sql
DESCRIBE Rides;
```
### Example 1: Filter
Let's suppose that you want to check the driving records in New York now.
isInNYC Is a UDF function.

```sql
SELECT * FROM Rides WHERE isInNYC(lon, lat);
```
You can also go to http://localhost:8081 to see how the Flink job is running.
```sql
http://localhost:8081
```

### Example 2: Group Aggregate

quantity of rides group by the quantity of passengers in the ride.
```sql
SELECT psgCnt, COUNT(*) AS cnt 
FROM Rides 
WHERE isInNYC(lon, lat)
GROUP BY psgCnt;
```

### Example 2: Window Aggregate

To continuously monitor the traffic flow in New York, you need to compute the number of vehicles that enter each area every five minutes. You want to focus only and mainly on the areas where at least 5 cars enter the city.
This process involves window aggregation (every five minutes), so the Tumbling Window syntax is required.


```sql
SELECT 
  toAreaId(lon, lat) AS area, 
  TUMBLE_END(rideTime, INTERVAL '1' MINUTE) AS window_end, 
  COUNT(*) AS cnt 
FROM Rides 
WHERE isInNYC(lon, lat)
GROUP BY 
  toAreaId(lon, lat), 
  TUMBLE(rideTime, INTERVAL '1' MINUTE) 
HAVING COUNT(*) >= 5;

```
"number of passengers for every 10 minutes"
```sql

SELECT 
  TUMBLE_START(rideTime, INTERVAL '10' MINUTE) AS cntStart,  
  TUMBLE_END(rideTime, INTERVAL '10' MINUTE) AS cntEnd,
  CAST(SUM(psgCnt) AS BIGINT) AS cnt 
FROM Rides 
GROUP BY TUMBLE(rideTime, INTERVAL '10' MINUTE);
```

### Example 3: Write the Append Stream to Kafka

Let's suppose you want to write the "number of passengers for every 10 minutes" stream to Kafka.
apache-flink-kafka-1
```sql
# container name apache-flink-kafka-1
docker ps
docker exec -it da916 bash
# 1 . Create a topic ridesToKafka
# 2 . Create a consumer to consume ridesToKafka topic
kafka-console-consumer.sh --bootstrap-server kafka:9092 --topic ridesToKafka --from-beginning
```

Create a FlinkTable using sqlflink client.

```sql
 CREATE TABLE ridesToKafka (
  `psgCnt` INT,
  `rideId` BIGINT,
  `taxiId` BIGINT,
  `isStart` BOOLEAN
) WITH (
  'connector' = 'kafka',
  'topic' = 'ridesToKafka',
  'properties.bootstrap.servers' = 'kafka:9092',
  'properties.group.id' = 'testGroup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'csv'
);
```
Crete a query in order to insert intermediate table connected with kafa topic.

```sql
 
 INSERT INTO ridesToKafka 
SELECT 
  psgCnt,  
  rideId,
  taxiId,
  isStart
FROM Rides;
 
```