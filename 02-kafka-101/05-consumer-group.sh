
kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello-world --property print.offset=true --group ps

    
kafka-consumer-groups.sh --bootstrap-server localhost:9092 --list