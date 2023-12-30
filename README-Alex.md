# My notes on Hudi

## Getting Started

Using these notes to get started:

- [Developer Setup](https://hudi.apache.org/contribute/developer-setup)

What I'd like to see:

- Can I use the docker that "comes with" Intellij?
- The Intellij setup is way too complicated.
- This [Developer Setup](https://hudi.apache.org/contribute/developer-setup)
  guide recommends "building as per instructions on Spark quickstart or Flink
  quickstart".  The [Spark quickstart
  guide](https://hudi.apache.org/docs/quick-start-guide/) links
  [here](https://spark.apache.org/downloads) for setting up Spark, which asks
  user to choolse a package type.  Which package type should be used?


to follow spark setup.


### My steps

Checked out hudi tag `hudi/release-0.14.1`.

Installed Spark 3.4 w/o Hadoop in home dir `~/spark-3.4.2-bin-without-hadoop/`.

Need python3, and pyspark package.  So create a virtual env:

```
cd ~/spark-3.4.2-bin-without-hadoop

rm -fr venv.build
mkdir venv.build
python3 -m venv venv.build
source venv.build/bin/activate

echo 'pyspark==3.4.2' > requirements.txt
python3 -m pip install -r requirements.txt
```



