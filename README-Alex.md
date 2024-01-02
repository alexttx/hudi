# My notes on Hudi

## Questions / TODO

- Can I use the docker that "comes with" Intellij?
- Use Spark
- Use Flink
- What is recommended Hudi dev system?


## My Progress

### 2024-01-02 TLDR Recipe to build and run docker demo

Ensure Maven uses Java 8: `mvn -v`

Clean repo and build:
```
mvn clean package -DskipTests
mvn pre-integration-test -DskipTests -Ddocker.compose.skip=true -Ddocker.build.skip=false
cd docker
sudo ./setup_demo.sh
```

Error:
```
Unable to load native-hadoop library for your platform... \
    using builtin-java classes where applicable
Local jar /var/hoodie/ws/docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar does not exist
```



### 2023-12-30 Docker demo use IN PROGRESS

CRAP: SET JAVA_HOME !!!!

Use `kcat` or `kafkacat` to put data into kafka.

Use Hoodie Streamer to stream from kafka to Hudi tables.

`spark-submit` failed bc file
`docker/hoodie/hadoop/hive_base/target/hoodie-utilities.jar` does not exist in
my built source tree. Aha.. the script `docker/build_local_docker_images.sh` has
commands to build them, but also recommends getting them from docker hub.

Maybe I skipped this step!

```
mvn clean package -Pintegration-tests -DskipTests
# Failed
```

The `docker/build_local_docker_images.sh` script has this:

```
mvn pre-integration-test -DskipTests -Ddocker.compose.skip=true -Ddocker.build.skip=false
# Passed
```

But you can also get them from docker hub (HOW?)


### 2023-12-30 Docker demo setup

https://hudi.apache.org/docs/docker_demo

Installed `jq`:
```
sudo apt install jq
```

Installed docker.  See `mynotes docker` on installing docker.

In hudi src:

```
cd docker
sudo ./setup_demo.sh   # should be able to do this as user?
```

Stop demo:

```
HUDI_WS=/home/alex/github/alexttx/hudi sudo -E docker-compose \
    -f /home/alex/github/alexttx/hudi/docker/compose/docker-compose_hadoop284_hive233_spark244.yml \
    down
```

Start demo:
```
HUDI_WS=/home/alex/github/alexttx/hudi sudo -E docker-compose \
    -f /home/alex/github/alexttx/hudi/docker/compose/docker-compose_hadoop284_hive233_spark244.yml \
    up
```

### 2023-12-29 Installed Spark 3.4 w/o Hadoop

WARNING: Not yet sure if this is the right setup for using Spark w/ Hudi

Installed in home dir via a tarball.

```
~/spark-3.2.4-bin-without-hadoop/
```

Spark needs `python3`, and `pyspark` package.  So I created a virtual env:

```
cd ~/spark-3.2.4-bin-without-hadoop

rm -fr venv.build
mkdir venv.build
python3 -m venv venv.build
source venv.build/bin/activate

echo 'pyspark==3.2.0' > requirements.txt
python3 -m pip install -r requirements.txt
```

### 2023-12-29 First successful build

Forked repo.

Built `hudi/release-0.14.1`.

Had to install Java 8, and set JAVA_HOME and PATH:

```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn -v
mvn clean package -DskipTests
```

Relying on JAVA_HOME and PATH is error prone. A safer method to force use of
Java 8 is to change the system's default.


```
sudo  update-alternatives --config java
```

See
[here](https://documentation.suse.com/sles/15-SP1/html/SLES-all/cha-update-alternative.html)
for more info.

**HOLY SHIT that didn't work!***  For the love of god, just purge other Java installs:

```
sudo apt purge openjdk-11*
sudo apt purge openjdk-18*
...
```

## References

- [Developer Setup](https://hudi.apache.org/contribute/developer-setup)

