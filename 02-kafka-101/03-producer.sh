to produce the messages

kafka-console-producer.sh --bootstrap-server localhost:9092 --topic hello-wd   --property key.separator=: --property parse.key=true

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello-wd   --property print.offset=true --property print.key=true  --group ps