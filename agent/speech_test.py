#!/usr/bin/env python
#-*- coding: utf-8 -*-
import pika
import uuid, json

class RpcSample(object):
    def __init__(self):
        credentials = pika.PlainCredentials('guest','wmind2017')
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.200.123.60', credentials=credentials))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True,
                                   queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, text):
        self.response = None
        self.corr_id = str(uuid.uuid4())
        self.channel.basic_publish(exchange='',
                                   routing_key='chatbot_simulator',
                                   properties=pika.BasicProperties(
                                         reply_to = self.callback_queue,
                                         correlation_id = self.corr_id,
                                         ),
                                   body=text)
        while self.response is None:
            self.connection.process_data_events()
        return self.response

rpc_sample = RpcSample()

credentials = pika.PlainCredentials('guest','wmind2017')
connection = pika.BlockingConnection(pika.ConnectionParameters(host='35.200.123.60', credentials=credentials))
channel = connection.channel()
channel.queue_declare(queue='speech_queue', durable=True)
print(' [*] Waiting for messages. To exit press CTRL+C')

def callback(ch, method, properties, body):
    body_json = {"user_text":body.decode('utf-8')}
    body_str = json.dumps(body_json)

    print(" [x] Requesting %s" % body_str)
    response = json.loads(rpc_sample.call(body_str))
    response_str = json.dumps(response, indent=4)

    print(" [.] Got\n %s" % response_str)
    print("Text:{}".format(response['bot_text']))

    ch.basic_ack(delivery_tag = method.delivery_tag)

channel.basic_qos(prefetch_count=1)
channel.basic_consume(callback, queue='speech_queue')

channel.start_consuming()


