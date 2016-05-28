# Exporting cAdvisor Stats to OpenTSDB

cAdvisor supports exporting stats to [OpenTSDB](http://opentsdb.net/). OpenTSDB is a Time Series Database based on HBase.

To use OpenTSDB, you need to pass some additional flags to cAdvisor telling it where the OpenTSDB instance is located:

Set the storage driver as OpenTSDB.

```
 -storage_driver=opentsdb
```

Specify what OpenTSDB instance to push data to:

```
 # The *ip:port* of the database. Default is 'localhost:8086'
 -storage_driver_host=ip:port
```

For example:

```
docker run \
  --volume=/:/rootfs:ro \
  --volume=/var/run:/var/run:rw \
  --volume=/sys:/sys:ro \
  --volume=/var/lib/docker/:/var/lib/docker:ro \
  --publish=8080:8080 \
  --detach=true \
  --name=cadvisor \
  google/cadvisor:latest \
  -storage_driver=opentsdb \
  -storage_driver_host=10.128.7.214:4242
```

# Examples

TODO
