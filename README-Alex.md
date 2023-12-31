# My notes on Hudi

## Questions / TODO

- Can I use the docker that "comes with" Intellij?
- Use Spark
- Use Flink
- What is recommended Hudi dev system?


## My Progress

### 2023-12-30 Docker demo

https://hudi.apache.org/docs/docker_demo

Installed jq and various docker packages:

```
sudo apt install jq
sudo apt install docker docker-compose
```

In hudi src:

```
cd docker
./setup_demo.sh  # as root, as user?
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

## References

- [Developer Setup](https://hudi.apache.org/contribute/developer-setup)

