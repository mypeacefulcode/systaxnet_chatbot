#!/usr/bin/env python
#-*- coding: utf-8 -*-

import time, os, sys
import logging, getopt
from logging.handlers import TimedRotatingFileHandler
import traceback, kazoo, pika
from tendo import singleton
from config.base import *
from lib.chat_loader import ChatLoader
from kazoo.client import KazooClient
from pymongo import MongoClient

def blocking_connection():
    credentials = pika.PlainCredentials(rabbitmq_conf['user'],rabbitmq_conf['password'])
    rb_conn = pika.BlockingConnection(pika.ConnectionParameters(host=rabbitmq_conf['hosts'], credentials=credentials))
    rb_channel = rb_conn.channel()
    rb_channel.queue_declare(queue='chat_queue', durable=True)

    return rb_channel

def main():
    logger.info("Program started!")

    # Zookeeper
    zk = KazooClient(hosts='%s, %s, %s'%(zk_server_1, zk_server_2, zk_server_3))
    zk.start()

    # MongoDB
    mongodb_conn = MongoClient(mongodb_conf['hosts'],mongodb_conf['port'])
    mongodb = mongodb_conn.rocketchat

    # RabbitMQ
    rb_channel = blocking_connection()

    cl = ChatLoader(zk, mongodb, rb_channel)

    while True:
        lock = zk.Lock(ZOOKEEPER_LOCK_PATH, os.getpid())
        with lock:
            position = cl.load_position()
            print("Position is: %s" % position)

            messages, max_ts = cl.load_data(position)

            print("Get data: %s" % messages)
            try:
                cl.put_data(messages)
            except:
                rb_channel = blocking_connection()
                cl = ChatLoader(zk, mongodb, rb_channel)
                cl.put_data(messages)

            cl.set_position(max_ts)
            sys.stdout.flush()
        time.sleep(1)

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
