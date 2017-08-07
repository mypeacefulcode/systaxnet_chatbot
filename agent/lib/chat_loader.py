#-*- coding: utf-8 -*-

import traceback, kazoo, pika, time
from config.base import *
from datetime import datetime
from bson import json_util
import json

class ChatLoader:
    def __init__(self, zk, mongodb, rb_channel):
        self.zk = zk
        self.mongodb = mongodb
        self.rb_channel = rb_channel

    def load_position(self):
        if not self.zk.exists(ZOOKEEPER_CONFIG_PATH + "/" + ZOOKEEPER_SAVED_POSITION):
            self.zk.ensure_path(ZOOKEEPER_CONFIG_PATH)
            self.set_position("0")

        data, _ = self.zk.get(ZOOKEEPER_CONFIG_PATH + "/" + ZOOKEEPER_SAVED_POSITION)

        return data.decode('utf-8')

    def set_position(self, position):
        position = position.strftime("%Y-%m-%d %H:%M:%S.%f")
        self.zk.set(ZOOKEEPER_CONFIG_PATH + "/" + ZOOKEEPER_SAVED_POSITION, position.encode())

    def load_data(self, position):
        try:
            min_ts = datetime.strptime(position, '%Y-%m-%d %H:%M:%S.%f')
        except ValueError:
            traceback.print_exc()
            min_ts = datetime.fromtimestamp(0)

        result = self.mongodb.rocketchat_message.find_one(sort=[("ts",-1)])
        max_ts = result['ts']

        cursor = self.mongodb.rocketchat_message.find({"$and":[{"ts":{"$gt":min_ts}},{"ts":{"$lte":max_ts}}]})
        messages = []
        for doc in cursor:
            if not doc['u']['username'] in CHATBOT_USER:
                messages.append(doc)
        cursor.close()

        return messages, max_ts

    def put_data(self, messages):
        for message in messages:
            body = json.dumps(message, default=json_util.default)
            self.rb_channel.basic_publish(exchange='',
                               routing_key='chat_queue',
                               body=body,
                               properties=pika.BasicProperties(
                                 delivery_mode = 2,
                               ))
