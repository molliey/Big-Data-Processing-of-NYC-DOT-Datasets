Firsly, we need to put the data into hdfs, then we can use the hadoop to process the data.
Notice: we should upload the data to our Dataproc cluster.

```shell
hdfs dfs -put DRI_Subindices_Indicators.csv /user/cw4450_nyu_edu/hiveInput/
```

```shell
hdfs dfs -ls /user/cw4450_nyu_edu/hiveInput/
```

Then we need to compile the java code and package it into a jar file.

```shell
javac -classpath `hadoop classpath` CategoryMapping.java
jar cvf categoryMapping.jar *.class
```

Then we can use the hadoop to process the data.

```shell
hadoop jar categoryMapping.jar CategoryMapping \
hdfs:///user/cw4450_nyu_edu/hiveInput/DRI_Subindices_Indicators.csv \
hdfs:///user/cw4450_nyu_edu/hiveOutput/cleaned_data 2>&1 | tee debug.log
```

Then we can check the result in hdfs.

```shell
hdfs dfs -ls /user/cw4450_nyu_edu/hiveOutput/cleaned_data
```

```shell
hdfs dfs -cat /user/cw4450_nyu_edu/hiveOutput/cleaned_data/part-r-00000
```

Then we can get the result from hdfs to local.

```shell
hdfs dfs -get /user/cw4450_nyu_edu/hiveOutput/cleaned_data/part-r-00000 ./mapped_data.txt
```

Finally, we can convert the result to a csv file.

```shell
python convert_to_csv.py
```

