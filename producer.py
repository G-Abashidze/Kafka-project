import time
from confluent_kafka import Producer




def delivery_report(err, msg):
	if err is not None:
		print(f"Message delivery failed: {err}")
	else:
		print(f"Message delivered to topic: {msg.topic()}; offset: {msg.offset()}; Value: {msg.value()} ")



def producer_messages():
	producer_conf = {
		'bootstrap.servers': 'localhost'
	}

	producer = Producer(producer_conf)

	counter = 0

	while 1:
		try:
			key = f"Key_{counter}"
			value = f"Value_{counter}"

			producer.produce('test_topic', key=key, value=value, callback=delivery_report)

			producer.poll(0)
			producer.flush()
			counter+=1
			
			time.sleep(3)

		except Exception as e:
			print(F"Error producing messages: {str(e)}")
		finally:
			producer.flush()

if __name__ == "__main__":
	producer_messages()
