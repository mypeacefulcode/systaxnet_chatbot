#-*- coding: utf-8 -*-

import pika, uuid

class QueueClient(object):
    def __init__(self, rabbitmq_conf, queue_name):
        self.rabbitmq_conf = rabbitmq_conf
        self.queue_name = queue_name
        self.connect()

    def connect(self):
        credentials = pika.PlainCredentials(self.rabbitmq_conf['user'],self.rabbitmq_conf['password'])

        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=self.rabbitmq_conf['hosts'], credentials=credentials))

        self.channel = self.connection.channel()

        result = self.channel.queue_declare(exclusive=True)
        self.callback_queue = result.method.queue

        self.channel.basic_consume(self.on_response, no_ack=True, queue=self.callback_queue)

    def on_response(self, ch, method, props, body):
        if self.corr_id == props.correlation_id:
            self.response = body

    def call(self, text):
        self.response = None
        self.corr_id = str(uuid.uuid4())

        try:
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       properties=pika.BasicProperties(
                                             reply_to = self.callback_queue,
                                             correlation_id = self.corr_id,
                                             ),
                                       body=text)
        except:
            self.connect()
            self.channel.basic_publish(exchange='',
                                       routing_key=self.queue_name,
                                       properties=pika.BasicProperties(
                                             reply_to = self.callback_queue,
                                             correlation_id = self.corr_id,
                                             ),
                                       body=text)

        while self.response is None:
            self.connection.process_data_events()

        return self.response
