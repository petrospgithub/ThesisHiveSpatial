spark-submit --class ThesisServer --master yarn --deploy-mode cluster target/HiveThrift-jar-with-dependencies.jar false

spark-submit --class ThesisServer --master yarn --deploy-mode client target/HiveThrift-jar-with-dependencies.jar false

!connect jdbc:hive2://172.18.0.2:10000 di thesis

ADD JAR hdfs://172.18.0.2:9000/user/root/esri-geometry-api-1.2.1.jar;
ADD JAR hdfs://172.18.0.2:9000/user/root/spatial-sdk-hive-1.2.1-SNAPSHOT.jar;
ADD JAR hdfs://172.18.0.2:9000/user/root/spatial-sdk-json-1.2.1-SNAPSHOT.jar;

