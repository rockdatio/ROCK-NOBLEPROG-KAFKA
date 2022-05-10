**⚠️ This repository has been archived. ⚠️**

---

# Kafka Connect Trainning

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
docker-compose up -d
```
Attach any broker

```sql
docker exec -it 8cfa6bf67679 bash
```


```sql
kafka-topics --bootstrap-server kafka1:19092,kafka2:19093,kafka3:19094 --topic file-sink-test --create --replication-factor 3 --partitions 3
```


```sql
connect-distributed /tmp/data/connect-distributed.properties
```
Probar con kafka1 kafka2 o kafka3 por problemas de intermitencia

```sql
curl -X POST -H "Content-Type: application/json" http://kafka1:8083/connectors --data @/tmp/data/connect-file-sink.json
```
*** PASO OPCIONAL SI ESTA MAL EL CONNECTOR INSTACIADO DEL PASO ANTERIOR ***
```sql
curl -X DELETE http://kafka1:8083/connectors/file-sink-test
```
**** EMITIR MENSAJES CON UN CONSUMIDOR O EJECUTAR EL JOB ESTRESADOR O GENERADOR DE TRAMAS
```sql
kafka-console-producer --broker-list kafka1:19092,kafka2:19093,kafka3:19094 --topic file-sink-test
```



