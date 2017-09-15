#!/usr/bin/env python
#-*- coding: utf-8 -*-

import time, os, sys, re
import logging, getopt, uuid
from logging.handlers import TimedRotatingFileHandler
import traceback, kazoo, pika
from tendo import singleton
from config.base import *
from lib.rocket_chat import RocketChat 
from lib.syntaxnet import Syntaxnet
from lib.analyze_text import AnalyzeText
from lib.execution_structure import ExecutionStructure
from kazoo.client import KazooClient
from pymongo import MongoClient
from rivescript import RiveScript
import json
import redis

class PublishingMessage(object):
    def __init__(self, zk, rs, rc, es, mongodb, redisdb, logger):
        self.zk = zk
        self.mongodb = mongodb
        self.redisdb = redisdb 
        self.rs = rs
        self.rc = rc
        self.es = es
        self.logger = logger
        self.kinds = ['when','what','how','why']
        self.regex = re.compile(r"(^\||\|$)")

    def callback(self, ch, method, properties, body, analyzer, syntaxnet):
        doc = json.loads(body.decode('utf-8'))
        print(doc)
        rc_channel = doc['rid']
        _uid = doc['u']['_id']

        lock = self.get_lock(_uid)
        if not lock.is_acquired:
            reply = self.rs.reply("localuser", MSG_PROCESSING_ALREADY)
        else:
            response = syntaxnet.analyze_syntax(doc['msg'])
            if not 'error' in response.keys():
                result = syntaxnet.token_to_string(response)
                df = syntaxnet.token_to_dataframe(response)

                es_response = self.es.read_parse_tree(df)
                print(es_response)

                corpus = []
                for kind in self.kinds:
                    words = es_response[kind]['words'] if es_response[kind]['words'] != None else ''
                    corpus.append(words)

                response = analyzer.call(' -+- '.join(corpus)).decode('utf-8').split(":@")

                idx = 0
                for morphs in response[1].split("-+-/None/Punctuation"):
                    morph_str = self.regex.sub('',morphs)
                    es_response[self.kinds[idx]]['morphs']  = morph_str
                    idx += 1

                print(es_response)
                context, sub_context, response  = self.es.read_intent(es_response, _uid)
                check_dict = self.es.check_domain(context, sub_context, _uid)
                formatter = self.es.make_formatter(context, sub_context, response, check_dict)

                self.es.set_user_context(_uid, context, sub_context, response, formatter)

                reply = self.make_reply(formatter)
            else:
                self.logger.error(response['error'])
                reply = SYSTEM_ERROR_MESSAGE

            self.rc.send_message(rc_channel, reply)

        self.release_lock(lock)
        sys.stdout.flush()

    def make_reply(self, message):
        reply = self.rs.reply("localuser", message)
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

    analyzer = AnalyzeText(rabbitmq_conf, 'morph_queue')
    syntaxnet = Syntaxnet(logger)

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

    # Redis
    #redisdb = redis.Redis(redis_conf['hosts'])
    redisdb = redis.StrictRedis(host=redis_conf['hosts'], charset="utf-8", decode_responses=True)

    # Rivescript
    rs = RiveScript(utf8=True)
    rs.load_directory("./rule")
    rs.sort_replies()

    # Rocket.chat
    rc = RocketChat(RC_CONF, logger)

    # Execution structure
    es = ExecutionStructure(ES_CONFIG, redisdb, logger)

    publisher = PublishingMessage(zk, rs, rc, es, mongodb, redisdb, logger)

    # RabbitMQ
    #while True:
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
