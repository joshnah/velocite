```
pip install -r requirements.txt
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0,com.datastax.spark:spark-cassandra-connector_2.12:3.4.1,com.github.jnr:jnr-posix:3.1.15 streaming.py localhost:9092 api_result 
```