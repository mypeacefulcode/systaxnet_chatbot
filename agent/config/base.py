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
        'parse_label_file':'parse_label.csv',
        'domain_exp_file':'domain_expression.csv',
    },
    # Context 
    # [환불, 주문취소, 배송, 기타]
    'context' : {
        'cancel-order': {
            'conditions' : ['exists_order','before_delivery'],
            'action' : ['cancel-order']
        },
        'refund' : {
            'conditions' : ['exists_order'],
            'action' : ['refund']
        },
        'delivery' : {
            'conditions' : ['exists_order'],
            'action' : ['delivery']
        },
        'etc' : {
            'conditions' : [],
            'action' : ['etc']
        }
    },
    'context-expression' : {
        'cancel-order': [
            {}
        ],
        'delivery' : [
            {'reason_entity':'늦다','purpose_entity':'thiing','action_entity':'오다'}
        ]
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
    },
    "special_obj" : ['order','cancel','refund'],
    "special_act" : ['하다']
}
