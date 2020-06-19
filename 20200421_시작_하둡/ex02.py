# pip install kafka-python

import time, threading, multiprocessing
from kafka import KafkaConsumer, KafkaProducer

class Consumer(multiprocessing.Process) : 
    def __init__(self) :
        multiprocessing.Process.__init__(self)
        self.stop_event = multiprocessing.Event()
        # 스레드를 초기화

    def stop(self) : 
        self.stop_event.set()

    def run(self) :
        # auto_offset_reset => latest(마지막), earliest(처음부터) 
        consumer = KafkaConsumer(bootstrap_servers='192.168.0.57',
            auto_offset_reset='latest', consumer_timeout_ms=1000)
        consumer.subscribe(['testTopic2'])
        while not self.stop_event.is_set() :
            for msg in consumer : 
                str = (msg.value).decode('utf-8')
                print(str+'Y') 

                if self.stop_event.is_set() :
                    break
        consumer.close()

class Producer(threading.Thread) : 
    def __init__(self) : 
        threading.Thread.__init__(self)
        self.stop_event = threading.Event()

    def stop(self) :
        self.stop_event.set()

    def run(self) : 
        producer = KafkaProducer(bootstrap_servers='192.168.0.57:9092')

        while not self.stop_event.is_set() : 
            str = input('send msg : ')
            producer.send('testTopic2', str.encode()) # string to byte 
            time.sleep(3)
        
        producer.close()

def main() : 
    tasks = [Consumer(), Producer()] 
    # [Consumer(),34,5,6,2] - 5개가 도는 것임 

    for tmp in tasks : 
        tmp.start()
    time.sleep(1000) 

    # 아래 두 개는 안전하게 종료할 수 있기 위해(옵션)
    # 스레드는 강제 종료 시키면 에러 뜬다 
    for tmp in tasks : 
        tmp.stop()

    for tmp in tasks : 
        tmp.join() 


if __name__ == '__main__' : 
    main()


# 받는 거 : 파이썬 / 보내는 거 : 우분투 
