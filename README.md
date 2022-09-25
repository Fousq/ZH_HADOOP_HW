## SSH

1. Check service status: sudo systemctl status ssh
2. Run service ssh: sudo systemctl status openssh

## HADOOP CLUSTER

1. Run dfs: $HADOOP_HOME/sbin/start-dfs.sh (if error, can help: hdfs namenode -format -force)
2. Run yarn: $HADOOP_HOME/sbin/start-yarn.sh

## RUN WordCount

1. Put files: hdfs dfs -put ./input/* /user/zhanbolat/wordcount/input
2. Delete dir(otherwise error): hdfs dfs -rmr /user/zhanbolat/wordcount/output
3. Compile class: hadoop com.sun.tools.javac.Main WordCount.java
4. Build JAR: jar cf wc.jar WordCount*.class
5. Run JOB: hadoop jar wc.jar WordCount /user/zhanbolat/wordcount/input /user/zhanbolat/wordcount/output

## RUN SortWordCountJob

1. Put files: hdfs dfs -put ./sort/* /user/zhanbolat/sort/input
2. Delete dir(otherwise error): hdfs dfs -rmr /user/zhanbolat/sort/output
3. Compile class: hadoop com.sun.tools.javac.Main SortWordCountJob.java
4. Build JAR: jar cf sort_wc.jar SortWordCountJob*.class
5. Run JOB: hadoop jar sort_wc.jar SortWordCountJob /user/zhanbolat/sort/input /user/zhanbolat/sort/output