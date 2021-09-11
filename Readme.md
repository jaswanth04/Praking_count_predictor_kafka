

Run 

```
docker compose build
```

Modify the mounts in docker-compose.yaml

before that create a folder by name data and also replace the volume mount in the spark-node with the appropriate folder structure
Also create a folder by name models in the above created data folder

Modify the mount for cassandra such that it reflects your local mount

To build the model, please go to the spark-model/Dockerfile and uncomment the line to build the model, first

Run

```
docker compose up
```

Later go to http://localhost:4000

Select the options for inputs and when you click on the predict, a json is created and then put in the cassandra store

To check for the files in cassandra store

Run

```
docker exec -it SEDS_Hacakthon2_cass_1 bash

#Takes you inside the container
cqlsh

# Takes you inside the cassandra prompt

SELECT * FROM parking_keyspace.violation_details ;
```

