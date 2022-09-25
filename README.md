## SSH

1. Check service status: sudo systemctl status ssh
2. Run service ssh: sudo systemctl status openssh

## HADOOP CLUSTER

1. Run dfs: $HADOOP_HOME/sbin/start-dfs.sh (if error, can help: hdfs namenode -format -force)
2. Run yarn: $HADOOP_HOME/sbin/start-yarn.sh

## RUN WeatherJob

1. Put files: hdfs dfs -put ./input/* /user/zhanbolat/weather/input
2. Delete dir(otherwise error): hdfs dfs -rmr /user/zhanbolat/weather/output
3. Compile class: hadoop com.sun.tools.javac.Main WeatherJob.java
4. Build JAR: jar cf weather.jar WeatherJob*.class
5. Run JOB: hadoop jar weather.jar WeatherJob /user/zhanbolat/weather/input /user/zhanbolat/weather/output