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
from lib.queue_client import QueueClient
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

    def get_message(self, body):
        doc = json.loads(body.decode('utf-8'))
        msg = {}
        if CHAT_TARGET == 'R':
            msg['uid'] = doc['u']['_id']
            msg['room_id'] = doc['rid']
            msg['text'] = doc['msg']
        else:
            msg['uid'] = "test_uid"
            msg['room_id'] = ""
            msg['text'] = doc['user_text']

        return msg

    def callback(self, ch, method, properties, body, analyzer, parser, syntaxnet):
        msg = self.get_message(body)

        lock = self.get_lock(msg['uid'])
        if not lock.is_acquired:
            reply = self.rs.reply("localuser", MSG_PROCESSING_ALREADY)
        else:
            if msg['text'][:2] == "&&":
                formatter = msg['text'].replace('\n','&enter ')
                print("formatter:",formatter)
                reply = self.make_reply(formatter).replace('&enter ','\n')
            else:
                try:
                    response = syntaxnet.analyze_syntax(msg['text'])
                    syntaxnet.save_respons(self.mongodb, response, msg['text'], CHAT_TARGET)
                    if 'error' in response.keys():
                        raise Exception(response['error'])

                    result = syntaxnet.token_to_string(response)
                    df = syntaxnet.token_to_dataframe(response)
    
                    segmentation_str = syntaxnet.verify_segmentation(response, msg['text'], self.es.verify_dict)
                    local_str = parser.call(segmentation_str).decode('utf-8')
                    local_df = syntaxnet.token_to_dataframe(json.loads(local_str))

                    es_response = self.es.make_execution_structure(local_df, analyzer)

                    domain, answer, params = self.es.read_intent(es_response, msg['uid'])
                    print("domain:{}, answer:{}".format(domain, answer))

                    if domain:
                        #check_dict = self.es.check_domain(domain, context, msg['uid'])
                        #formatter = self.es.make_formatter(domain, context, check_dict)
                        formatter = ' '.join([domain, answer])
                    else:
                        formatter = ""

                    self.es.save_user_context(msg['uid'], params)
                    print("formatter:", formatter)
                    reply = self.make_reply(formatter)
                except:
                    self.logger.error(traceback.print_exc())
                    sys.stdout.flush()

                    formatter = "help do"
                    reply = self.make_reply(formatter)
                    #reply = SYSTEM_ERROR_MESSAGE

            self.send_message(msg, reply, ch, method, properties)

        self.release_lock(lock)
        sys.stdout.flush()

    def send_message(self, msg, reply, ch, method, props):
        if CHAT_TARGET == 'R':
            self.rc.send_message(msg['room_id'], reply)
        elif CHAT_TARGET == 'S':
            response = {
                'bot_text':reply,
                'result':True
            }
            ch.basic_publish(exchange='',
                             routing_key=props.reply_to,
                             properties=pika.BasicProperties(correlation_id = props.correlation_id),
                             body=json.dumps(response))
            ch.basic_ack(delivery_tag = method.delivery_tag)

    def make_reply(self, message):
        reply = self.rs.reply("localuser", message)
        return reply

    def get_lock(self, uid):
        test = self.zk.ensure_path(ZOOKEEPER_USER_LOCK_PATH + "/" +  uid)
        lock = self.zk.Lock(ZOOKEEPER_USER_LOCK_PATH + "/" +  uid, os.getpid())

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

    channel.queue_declare(queue=CHAT_QUEUE, durable=True)

    analyzer = QueueClient(rabbitmq_conf, 'morph_queue')
    syntaxnet = Syntaxnet(logger, publisher.es.entities)

    parser = QueueClient(rabbitmq_conf, 'nlp_queue')

    channel.basic_qos(prefetch_count=1)
    channel.basic_consume(lambda ch, method, properties, body:
            publisher.callback(ch, method, properties, body, analyzer, parser, syntaxnet), queue=CHAT_QUEUE, no_ack=QUEUE_ACK)

    channel.start_consuming()

def main():
    logger.info("Program started!")

    # Zookeeper
    zk = KazooClient(hosts='%s, %s, %s'%(zk_server_1, zk_server_2, zk_server_3))
    zk.start()

    # MongoDB
    mongodb = MongoClient(mongodb_conf['hosts'],mongodb_conf['port'])

    # Redis
    #redisdb = redis.Redis(redis_conf['hosts'])
    redisdb = redis.StrictRedis(host=redis_conf['hosts'], charset="utf-8", decode_responses=True)

    # Rivescript
    rs = RiveScript(utf8=True)
    rs.load_directory(RULE_DIR)
    rs.sort_replies()

    # Rocket.chat
    rc = RocketChat(RC_CONF, logger)

    # Execution structure
    es = ExecutionStructure(ES_CONFIG, redisdb, logger)

    publisher = PublishingMessage(zk, rs, rc, es, mongodb, redisdb, logger)

    # RabbitMQ
    # while True:
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
        opts, args = getopt.getopt(sys.argv[1:],"st:e:",["singleton","target=","env="])

        for opt, arg in opts:
            if opt in ('-s', '--singleton'):
                me = singleton.SingleInstance()
            if opt in ('-e', '--env'):
                if arg == "local":
                    from config.local import *
                elif arg == "development":
                    from config.development import *
                elif arg == "release":
                    from config.release import *
                elif arg == "production":
                    from config.production import *
                else:
                    raise Exception("Error: -e[--env] option")
            if opt in ('-t', '--type'):
                if arg == "simulator":
                    CHAT_TARGET = "S"
                    CHAT_QUEUE = "chatbot_simulator"
                    QUEUE_ACK = False
                    RULE_DIR  = "./rule/cs_set"
                else:
                    CHAT_TARGET = "R"
                    CHAT_QUEUE = "chat_queue"
                    QUEUE_ACK = True
                    RULE_DIR  = "./rule/in_house_set"

    except Exception:
        traceback.print_exc()
        logger.error(traceback.format_exc())

    main()
