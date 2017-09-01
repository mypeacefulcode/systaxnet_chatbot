#!/usr/bin/env python
#-*- coding: utf-8 -*-

import time, os, sys, re
import logging, getopt, uuid
from logging.handlers import TimedRotatingFileHandler
import traceback, kazoo, pika
from tendo import singleton
from config.base import *
from lib.rocket_chat import RocketChat 
from kazoo.client import KazooClient
from pymongo import MongoClient
from rivescript import RiveScript
import json

import googleapiclient.discovery

class Syntaxnet(object):
    def __init__(self):
        """Returns the encoding type that matches Python's native strings."""
        if sys.maxunicode == 65535:
            self.encoding = 'UTF16'
        else:
            self.encoding = 'UTF32'

        self.service = googleapiclient.discovery.build('language', 'v1')

    def analyze_syntax(self, text):
        body = {
            'document': {
                'type': 'PLAIN_TEXT',
                'content': text,
            },
            'encoding_type': self.encoding
        }

        request = self.service.documents().analyzeSyntax(body=body)
        response = request.execute()

        return response

    def token_to_string(self, response):
        result = ''
        idx = 0
        for token in response['tokens']:
            result += '[{}] {}: {}: {} ({})\n'.format(idx, token['partOfSpeech']['tag'], token['text']['content'],
                                               token['dependencyEdge']['headTokenIndex'], token['dependencyEdge']['label'])
            idx += 1

        return result

class AnalyzeText(object):
    def __init__(self, rabbitmq_conf):
        self.rabbitmq_conf = rabbitmq_conf
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
                                       routing_key='morph_queue',
                                       properties=pika.BasicProperties(
                                             reply_to = self.callback_queue,
                                             correlation_id = self.corr_id,
                                             ),
                                       body=text)
        except:
            self.connect()
            self.channel.basic_publish(exchange='',
                                       routing_key='morph_queue',
                                       properties=pika.BasicProperties(
                                             reply_to = self.callback_queue,
                                             correlation_id = self.corr_id,
                                             ),
                                       body=text)

        while self.response is None:
            self.connection.process_data_events()
        return self.response

class PublishingMessage(object):
    def __init__(self, zk, rs, rc, mongodb):
        self.zk = zk
        self.mongodb = mongodb
        self.rs = rs
        self.rc = rc

    def callback(self, ch, method, properties, body, analyzer, syntaxnet):
        doc = json.loads(body.decode('utf-8'))
        rc_channel = doc['rid']
        _uid = doc['u']['_id']

        response = analyzer.call(doc['msg']).decode('utf-8').split(":@")
        formatter = response[0]
        morph_str = response[1].replace('|','\n')

        reply = self.make_reply(formatter, _uid)

        response = syntaxnet.analyze_syntax(doc['msg'])
        result = syntaxnet.token_to_string(response)

        reply = formatter + "\n" + \
                "------ morph string -------\n" + \
                morph_str + "\n" +  \
                "------ parse tree -------\n" + \
                result + "\n" + \
                "------ reply -------\n" + \
                reply

        self.rc.send_message(rc_channel, reply)
        sys.stdout.flush()

    def make_reply(self, message, _uid):

        lock = self.get_lock(_uid)
        if lock.is_acquired:
            reply = self.rs.reply("localuser", message)
        else:
            reply = self.rs.reply("localuser", MSG_PROCESSING_ALREADY)
        self.release_lock(lock)

        return reply

    def get_lock(self, _uid):
        test = self.zk.ensure_path(ZOOKEEPER_USER_LOCK_PATH + "/" +  _uid)
        lock = self.zk.Lock(ZOOKEEPER_USER_LOCK_PATH + "/" +  _uid, os.getpid())

        try:
            lock.acquire(timeout=0.5)
        except kazoo.exceptions.LockTimeout:
            logger.error(traceback.print_exc())
            sys.stdout.flush()

        return lock

    def release_lock(self, lock):
        lock.release()

        return True

def blocking_connection(publisher):
    credentials = pika.PlainCredentials(rabbitmq_conf['user'],rabbitmq_conf['password'])

    connection = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_conf['hosts'], credentials=credentials))
    channel = connection.channel()

    channel.queue_declare(queue='chat_queue', durable=True)

    analyzer = AnalyzeText(rabbitmq_conf)
    syntaxnet = Syntaxnet()

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(lambda ch, method, properties, body:
            publisher.callback(ch, method, properties, body, analyzer, syntaxnet), queue='chat_queue', no_ack=True)

    channel.start_consuming()

def main():
    logger.info("Program started!")

    # Zookeeper
    zk = KazooClient(hosts='%s, %s, %s'%(zk_server_1, zk_server_2, zk_server_3))
    zk.start()

    # MongoDB
    mongodb_conn = MongoClient(mongodb_conf['hosts'],mongodb_conf['port'])
    mongodb = mongodb_conn.rocketchat

    # Rivescript
    rs = RiveScript(utf8=True)
    rs.load_directory("./rule")
    rs.sort_replies()

    # Rocket.chat
    rc = RocketChat(RC_CONF, logger)

    publisher = PublishingMessage(zk, rs, rc, mongodb)

    # RabbitMQ
    while True:
        try: 
            logger.info("[*] Waiting for messages")
            blocking_connection(publisher)
        except:
            logger.error(traceback.print_exc())
            sys.stdout.flush()

if __name__ == "__main__":

    # 로그 설정
    logger = logging.getLogger('Log')
    logger.setLevel(logging.DEBUG)

    # 로그 포맷 설정
    path = './logs/%s-%s.log'%(os.path.basename(__file__),time.strftime("%Y-%m-%d"))
    fh = logging.FileHandler(path)
    fh.setLevel(logging.DEBUG)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    fh.setFormatter(formatter)
    logger.addHandler(fh)

    # 로그를 화면에도 출력
    sh = logging.StreamHandler()
    logger.addHandler(sh)

    # 시간으로 로그파일 로테이트 
    rh = TimedRotatingFileHandler(path, when="d", interval=1, backupCount=8)
    logger.addHandler(rh)

    # 실행 옵션 처리
    # -s(--singletone) 중복실행 방지 옵션
    # -e(--env) 개발환경 지정 옵션
    try:
        opts, args = getopt.getopt(sys.argv[1:],"se:",["singleton","env="])

        for opt, arg in opts:
            if opt in ('-s', '--singleton'):
                me = singleton.SingleInstance()
            if opt in ('-e', '--env'):
                if arg == "local":
                    from config.local import *
                elif arg == "development":
                    from config.development import *
                elif arg == "production":
                    from config.production import *
                else:
                    raise Exception("Error: -e[--env] option")

    except Exception:
        traceback.print_exc()
        logger.error(traceback.format_exc())

    main()
