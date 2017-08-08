#-*- coding: utf-8 -*-

import traceback, kazoo, pika, time
from config.base import *
from datetime import datetime
from bson import json_util
import json, requests

class RocketChat:
    def __init__(self, rc_conf, logger):
        self.rc_conf = rc_conf 
        self.logger = logger
        self.connection()

    def connection(self):
        url_str = self.rc_conf['url'] + self.rc_conf['path'] + '/login'
        data = {'username':self.rc_conf['username'], 'password':self.rc_conf['password']}

        while True:
            res = requests.post(url_str, data=data)
            res_json = res.json()

            if res.status_code != 200:
                self.logger.error("Error: %s (response code: %d)" % (res.text, res.status_code))
            elif res_json['status'] == "success":
                self.rc_token = res_json['data']['authToken']
                self.rc_userid = res_json['data']['userId']

                return True
            else:
                self.logger.error("%s , %s" %(res_json['status'], res_json['message']))
                time.sleep(5)

    def send_message(self, rc_channel, message):
        url_str = self.rc_conf['url'] + self.rc_conf['path'] + '/chat.postMessage'
        data = {'channel':'#'+rc_channel, 'text':message}

        headers = {'X-Auth-Token': self.rc_token, 'X-User-Id': self.rc_userid, 'Content-type':'application/json'}
        res = requests.post(url_str, json=data, headers=headers)

        return_value = True
        if res.status_code != 200:
            self.logger.error("Error: %s (response code: %d)" % (res.text, res.status_code))
            return_value = False

        return return_value
