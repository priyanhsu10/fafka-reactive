
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello-wd --property print.offset=true --group ps


kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list


kafka-consumer-groups.sh --bootstrap-server localhost:9092 --group ps --describe