# create topic 
kafka-topics.sh --bootstrap-server locahost:9092 --topic order-event --create

#  list all the topics
kafka-topics.sh --bootstrap-server localhost:9092 --list

# describe more details for topic
kafka-topic.sh --bootstrap-server localhost:9092 --topic order-event --describe

# delete the topic 
kafka-topic.sh --boostrap-server localhost:9092 --topic order-event --delete