# create topic 
kafka-topics.sh --bootstrap-server locahost:9092 --topic order-event --create

#  list all the topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# describe more details for topic
kafka-topics.sh --bootstrap-server localhost:9092 --topic order-event --describe

# delete the topic 
kafka-topics.sh --boostrap-server localhost:9092 --topic order-event --delete


kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --delete

kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic hello-world --property print.offset=true --group ps


kafka-topics.sh --bootstrap-server localhost:9092 --topic hello-world --create --partitions 2