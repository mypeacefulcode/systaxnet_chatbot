#!/usr/bin/env python
#-*- coding: utf-8 -*-
import pika
import uuid, json

class RpcSample(object):
    def __init__(self):
        credentials = pika.PlainCredentials('guest','wmind2017')
        mq_server = '35.189.129.153' #release
        #mq_server = '35.200.123.60' #development
        self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=mq_server, credentials=credentials))

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

texts = [
    "환불 할께요",
    "환불하고 싶어요",
    "환불하려면 어떻게 해야 하나요",
    "환불함",
    "주문취소",
    "주문취소할께",
    "주문취소 하고 싶어요",
    "주문취소 합니다",
    "전화번호 바꿀께요",
    "핸드폰 번호 바꾸고 싶어요",
    "전화번호 바꿀께",
    "전화번호 바꾸려면 어떻게 해야하나요",
    "전화번호 변경 요청할께요"
    "반품 요청한거 취소할께요",
    "메일 주소 바꿀께",
    "고객센터 연결해줘",
    "상담사 바꿔줘",
    "환불 가능하나요"
]
texts = ["메일 주소 바꿀께"]

for text in texts:
    body_json = {"user_text":text}
    body_str = json.dumps(body_json)

    print(" [x] Requesting %s" % body_str)
    response = json.loads(rpc_sample.call(body_str))
    response_str = json.dumps(response, indent=4)

    print(" [.] Got\n %s" % response_str)
    print("Text:{}".format(response['bot_text']))
