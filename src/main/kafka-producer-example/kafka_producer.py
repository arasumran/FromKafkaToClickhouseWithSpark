import random
import socket
import struct
from datetime import datetime
from json import dumps

from kafka.producer import KafkaProducer


def generate_random_number():
    import random
    return random.randint(0, 1)


def generate_datetime():
    import datetime
    return datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')


def send_data_to_kafka(producer, topic, data):
    try:
        producer.send(topic, value=dumps(data).encode('utf-8'))
        producer.flush()
        print('Message sent successfully')
    except Exception as ex:
        print('Exception occurred: {}'.format(str(ex)))


def random_source_generate():
    import random
    source_list = ['facebook', 'google', 'twitter', 'instagram', 'linkedin', 'pinterest', 'youtube', 'other']
    return random.choice(source_list)


def generate_random_referres_url():
    import random
    referres_url_list = ['https://www.facebook.com/', 'https://www.google.com/', 'https://www.twitter.com/',
                         'https://www.instagram.com/', 'https://www.linkedin.com/', 'https://www.pinterest.com/',
                         'https://www.youtube.com/']
    return random.choice(referres_url_list)


def generate_from_user_cookie_id_list():
    import random
    from_user_cookie_id_list = [567345678, 453678345, 8765532342323, 54645623423, 345346678768123, 12321323213,
                                865756775543, 3423434234176, 88888888888, 77777777777777777, 999999999999999,
                                55554444332, 9876543545345]
    return random.choice(from_user_cookie_id_list)


generate_random_referres_url_by_source = {'facebook': 'https://www.facebook.com/',
                                          'google': 'https://www.google.com/',
                                          'twitter': 'https://www.twitter.com/',
                                          'instagram': 'https://www.instagram.com/',
                                          'linkedin': 'https://www.linkedin.com/',
                                          'pinterest': 'https://www.pinterest.com/',
                                          'youtube': 'https://www.youtube.com/',
                                          'other': 'https://www.foreverything.com/'}


def generate_impression_by_source(source):
    if source == 'facebook':
        return 1000000
    elif source == 'google':
        return 10000000
    elif source == 'twitter':
        return 2000000
    elif source == 'instagram':
        return 3000000
    elif source == 'linkedin':
        return 4000000
    elif source == 'pinterest':
        return 5000000
    elif source == 'youtube':
        return 1000000
    elif source == 'other':
        return 5000


def generate_ip_address():
    return socket.inet_ntoa(struct.pack('>I', random.randint(1, 0xffffffff)))


if __name__ == "__main__":
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])
    # for i in range(1, 1000000):
    source = random_source_generate()
    data = {
        'user_cookie_id': generate_from_user_cookie_id_list(),
        'clicked': generate_random_number(),
        'ip': generate_ip_address(),
        'source': source,
        'date': generate_datetime(),
        'referrer': generate_random_referres_url_by_source[source],
        'impression': generate_impression_by_source(source)
    }
    print(data)
    print(datetime.now())
    send_data_to_kafka(producer, 'fornrt', data)
    producer.close()
