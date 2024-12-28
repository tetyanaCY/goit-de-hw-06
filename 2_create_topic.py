from kafka.admin import KafkaAdminClient, NewTopic
from kafka.errors import TopicAlreadyExistsError
from configs import kafka_config

# Connect to Kafka Admin Client
try:
    print("Connecting to Kafka Admin Client...")
    admin_client = KafkaAdminClient(
        bootstrap_servers=kafka_config['bootstrap_servers'],
        security_protocol=kafka_config['security_protocol'],
        sasl_mechanism=kafka_config['sasl_mechanism'],
        sasl_plain_username=kafka_config['username'],
        sasl_plain_password=kafka_config['password']
    )
    print("Connected to Kafka Admin Client successfully.")
except Exception as e:
    print(f"Failed to connect to Kafka Admin Client: {e}")
    exit(1)

# Topic definitions
my_name = "tati"
topic_name_in = f"{my_name}_iot_sensors_data"
alerts_topic_name = f"{my_name}_iot_alerts"
num_partitions = 2
replication_factor = 1

new_topics = [
    NewTopic(name=topic_name_in, num_partitions=num_partitions, replication_factor=replication_factor),
    NewTopic(name=alerts_topic_name, num_partitions=num_partitions, replication_factor=replication_factor)
]

try:
    # Get the list of existing topics
    existing_topics = admin_client.list_topics()
    print(f"Existing topics: {existing_topics}")

    # Filter out topics that already exist
    topics_to_create = [topic for topic in new_topics if topic.name not in existing_topics]

    if topics_to_create:
        print(f"Creating topics: {[topic.name for topic in topics_to_create]}...")
        admin_client.create_topics(new_topics=topics_to_create, validate_only=False)
        print("Topics created successfully.")
    else:
        print("All topics already exist. No topics were created.")
except TopicAlreadyExistsError as e:
    print(f"Some topics already exist: {e}")
except Exception as e:
    print(f"An error occurred while creating topics: {e}")
finally:
    print("Closing Kafka Admin Client...")
    admin_client.close()
    print("Kafka Admin Client closed successfully.")
