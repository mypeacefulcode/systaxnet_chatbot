ZOOKEEPER_CONFIG_PATH = "/chatbot-config"
ZOOKEEPER_LOCK_PATH = ZOOKEEPER_CONFIG_PATH + "/lock"
ZOOKEEPER_USER_LOCK_PATH = ZOOKEEPER_CONFIG_PATH + "/user-lock"
ZOOKEEPER_SAVED_POSITION = "db-position"

SYSTEM_ERROR_MESSAGE = "챗봇 시스템에 장애가 발생했습니다. 관리자에게 문의해 주십시오."

# ['chatbot']
CHATBOT_USER = ['MJCcwuRtcaDs9Mfe8']
RC_CONF = {
    'url':'http://35.189.156.233:3000',
    'path':'/api/v1',
    'username':'chatbot',
    'password':'wmind2017'
}

# Message
MSG_PROCESSING_ALREADY = "processing already"

# Execution structure configuration
ES_CONFIG = {
    'entities_config' : {
        'csv_path':'./data',
        'csv_entities_file':'entities.csv',
        'parse_label_file':'parse_label.csv'
    },
    # Context 
    # [환불, 주문취소, 배송, 기타]
    'context' : {
        'cancel-order': {
            'means' : [('cancel','order')],
            'conditions' : ['exists_order','before_delivery'],
            'action' : ['cancel-order']
        },
        'refund' : {
            'means' : [('refund',)],
            'conditions' : ['exists_order'],
            'action' : ['refund']
        },
        'delivery' : {
            'means' : [('delivery',)],
            'conditions' : ['exists_order'],
            'action' : ['delivery']
        },
        'etc' : {
            'means' : [('question',)],
            'conditions' : [],
            'action' : ['etc']
        }
    },
    # Means
    "means" : {
        'cancel' : {
            'attribute' : ['need-to-object']
        },
        'request' : {
            'attribute' : ['need-to-object']
        }
    },
    "validation_formatter" : {
        'exists_order' : 'error-exists-order',
        'before_delivery' : 'error-before-delivery'
    }
}
