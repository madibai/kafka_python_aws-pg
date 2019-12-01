from kafka import KafkaConsumer
import json
consumer = KafkaConsumer('python-test')
print("start")
for message in consumer:
    #print(message.value.decode())
    aa = json.loads(message.value.decode())
    print('currency: {}, amount: {}, branch: {}'.format(aa['currency'], aa['amount'], aa['branch']))