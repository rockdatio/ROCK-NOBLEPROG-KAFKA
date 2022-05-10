# Apache ksqldb SQL Training

### Requirements

Use Docker Compose to setup the training environment.

You **only need [Docker](https://www.docker.com/)** to run this training. </br>
You don't need Java, Scala, or an IDE.

### Getting the Docker Compose Configuration
Docker Compose environments are configured with a YAML file. The default path of the file is docker-compose.yml.

Get the configuration file for the training environment by cloning our Git repository
```sql
https://github.com/rockdatio/ROCK-NOBLEPROG-KAFKA.git
```

#### Levantar el cluster de kafka y ksqldb cli

```sql
docker-compose up
```

#### Entrar al contenedor con el cliente ksqldb-cli.
```sql
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
```

#### Luego de entrar al cliente ksqldb cli, crearemos el STREAM.

 ```sql
 CREATE STREAM riderLocations (profileId VARCHAR, latitude DOUBLE, longitude DOUBLE)
   WITH (kafka_topic='locations', value_format='json', partitions=1);
 ```

### validemos que se haya creado correctamente.

 ```sql
 SELECT * FROM riderLocations
  EMIT CHANGES;
 ```


### Creamos materialized views

 ```sql
 CREATE TABLE currentLocation AS
   SELECT profileId,
          LATEST_BY_OFFSET(latitude) AS la,
          LATEST_BY_OFFSET(longitude) AS lo
   FROM riderlocations
   GROUP BY profileId
   EMIT CHANGES;
 ```


  ########################################################3
  
```sql
  CREATE TABLE ridersNearMountainView AS
  SELECT ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1) AS distanceInMiles,
         COLLECT_LIST(profileId) AS riders,
         COUNT(*) AS count
  FROM currentLocation
  GROUP BY ROUND(GEO_DISTANCE(la, lo, 37.4133, -122.1162), -1);
 ```

####################################################################
  -- Mountain View lat, long: 37.4133, -122.1162
  
```sql
 SELECT * FROM riderLocations
   WHERE GEO_DISTANCE(latitude, longitude, 37.4133, -122.1162) <= 5 EMIT CHANGES;
 ```

#####################################################################
```sql
docker exec -it ksqldb-cli ksql http://ksqldb-server:8088
 ```


#####################################################################
```sql
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('c2309eec', 37.7877, -122.4205);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('18f4ea86', 37.3903, -122.0643);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ab5cbad', 37.3952, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('8b6eae59', 37.3944, -122.0813);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4a7c7b41', 37.4049, -122.0822);
INSERT INTO riderLocations (profileId, latitude, longitude) VALUES ('4ddad000', 37.7857, -122.4011);
 ```



###########################################################################
```sql
SELECT * from ridersNearMountainView WHERE distanceInMiles <= 10;
 ```
###########################################################################
