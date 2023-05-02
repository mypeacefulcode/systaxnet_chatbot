
# Syntaxnet

Syntaxnet은 2016년 구글에서 NLU(natural language understanding)를 위해 발표한 Framework 이며,  
문장을 POS(A part of speech)로 나누어 의존성 구분분석 트리(The dependency parse tree)를 만드는 것이 핵심 요소다.

<img width="600" alt="스크린샷 2023-05-02 오후 12 11 27" src="https://user-images.githubusercontent.com/16236194/235572106-a35b29e0-6e2f-445a-8d75-b5313b876c6b.png">
<sub>[출처 : 구글블로그](https://ai.googleblog.com/2016/05/announcing-syntaxnet-worlds-most.html?m=1)</sub>

# Syntaxnet chatbot

Syntaxnet chatbot은 사용자가 입력한 문장에서 의존성 구문분석 트리를 구하고 이 트리에서 **Subject, Object, Action** 세가지 요소를 추출하여 사용자의 Intent를 이해하고 상응하는 답변을 하는 시스템이다.

예)  
<img width="623" alt="스크린샷 2023-05-02 오후 2 00 09" src="https://user-images.githubusercontent.com/16236194/235582845-9738012c-02ec-4dff-9e66-d53b5ba3f6c7.png">

위의 의존성 구문분석 트리에서 do refund 라는 intent를 찾아내고 그에 맞는 채팅을 챗봇이 하게 된다.
