#!/usr/bin/env python

import sys, json
import googleapiclient.discovery

def get_native_encoding_type():
    """Returns the encoding type that matches Python's native strings."""
    if sys.maxunicode == 65535:
        return 'UTF16'
    else:
        return 'UTF32'

def analyze_syntax(text, encoding='UTF32'):
    body = {
        'document': {
            'type': 'PLAIN_TEXT',
            'content': text,
        },
        'encoding_type': encoding
    }

    service = googleapiclient.discovery.build('language', 'v1')

    request = service.documents().analyzeSyntax(body=body)
    response = request.execute()

    return response

text = "환불해주세요"
result = analyze_syntax(text, get_native_encoding_type())

for token in result['tokens']:
    #print(token)
    print(u'{}: {}: {} ({})'.format(token['partOfSpeech']['tag'], token['text']['content'],
                                   token['dependencyEdge']['headTokenIndex'], token['dependencyEdge']['label']))
