# Fraud streamhouse with Fluss, Flink and Iceberg


Disclaimer:<br>
I created this repository to experiment with Apache Fluss (Incubating). Please note that the cluster sizing, security configuration, fraud detection logic are intended for exploration only and are not production-ready.<br><br>


Fluss/Flik required JARs:<br>
https://fluss.apache.org/docs/streaming-lakehouse/integrate-data-lakes/iceberg/<br><br>


Fraud Job JAR packaging cmd:<br>
mvn package -f pom.xml<br><br>


Flink Job submit parameters:<br>
--parallelism 2 --fluss.bootstrap.servers coordinator-server:9122 --datalake.format iceberg --datalake.iceberg.type rest --datalake.iceberg.warehouse s3://fluss/data --datalake.iceberg.uri http://rest:8181 --datalake.iceberg.s3.endpoint http://minio:9000 --datalake.iceberg.s3.path-style-access true<br><br>


Docker commands:<br>
docker-compose down -v<br>
docker-compose up --build<br><br>


Flink SQL client commands:<br>
CREATE CATALOG fluss_catalog WITH (
    'type' = 'fluss',   
    'bootstrap.servers' = 'coordinator-server:9122'
);<br>
USE CATALOG fluss_catalog;<br>
SET 'execution.runtime-mode' = 'batch';<br>
SET 'sql-client.execution.result-mode' = 'tableau';<br>
select * from fraud;<br>
select * from fraud$lake;<br>





