from json import dumps
from time import sleep
from numpy.random import choice, randint
from kafka import KafkaProducer


def get_random_value():
    new_dict = {}
    branch_list = ["Almaty", "Astana", "Taraz", "Semei"]
    currency_list = ["KZT", "RUB", "GBP", "USD"]

    new_dict['city'] = choice(branch_list)
    new_dict['currency'] = choice(currency_list)
    new_dict['amount'] = randint(1, 100)

    return new_dict


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                             value_serializer=lambda x: dumps(x).encode('utf-8'),
                             compression_type='gzip')
    topic_name = 'transaction'

    while True:
        for _ in range(100):
            data = get_random_value()
            try:
                message = producer.send(topic=topic_name, value=data)
                record_data = message.get(timeout=10)
                print('data: {}, offset: {}' \
                      .format(data, record_data.offset))
                #print(data)
            except Exception as e:
                print(e)
            finally:
                producer.flush()
        sleep(13)
    producer.close()