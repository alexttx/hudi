# My notes on Hudi

## References

- [Developer Setup](https://hudi.apache.org/contribute/developer-setup)

## My Progress

### 2024-01-02 Run docker demo

Build branch `hudi/release-0.14.1`:
```
mvn clean package -Pintegration-tests -DskipTests -Dspark2.4 -Dscala-2.11
```

Bootstrap/Teardown demo:
```
cd docker
sudo ./setup_demo.sh   # wipes out state and restarts containers
sudo ./stop_demo.sh
```

Bring up images so you can see logs:
```
HUDI_WS=/home/alex/github/alexttx/hudi sudo -E docker-compose \
    -f /home/alex/github/alexttx/hudi/docker/compose/docker-compose_hadoop284_hive233_spark244.yml \
    up
```

Stop images:
```
HUDI_WS=/home/alex/github/alexttx/hudi sudo -E docker-compose \
    -f /home/alex/github/alexttx/hudi/docker/compose/docker-compose_hadoop284_hive233_spark244.yml \
    down
```

### 2023-12-29 Spark quick start (incomplete)

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


### 2023-12-30 Bulid Hudi

```
mvn clean package -DskipTests
```

### 2023-12-30 Prereqs: docker, kcat, jq

Install docker.  See `mynotes docker`.

Install kcat / kafkacat:
```
sudo apt install kafkacat
sudo ln -s $(which kafkacat) /usr/bin/kcat
```

Install `jq`:
```
sudo apt install jq
```

### 2023-12-29 Java Setup

Must use Java 8.

#### Option 1: Set JAVA_HOME and PATH:

```
export JAVA_HOME=/usr/lib/jvm/java-8-openjdk-amd64
export PATH=$JAVA_HOME/bin:$PATH
mvn -v
mvn clean package -DskipTests
```

Relying on JAVA_HOME and PATH is error prone. A safer method to force use of
Java 8 is to change the system's default.

### Option 2: set system default to Java 8

```
sudo  update-alternatives --config java
```

See
[here](https://documentation.suse.com/sles/15-SP1/html/SLES-all/cha-update-alternative.html)
for more info.

Well, WTF that doesn't work bc Java 8, 11 and 17 binaries all leak through into PATH.
So you get a mishmash of different JREs.

### Option 3: Nuke all Java's except Java 8

```
sudo apt purge openjdk-11*
sudo apt purge openjdk-18*
...
```

