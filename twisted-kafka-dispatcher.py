"""The most basic chat protocol possible.

run me with twistd -y chatserver.py, and then connect with multiple
telnet clients to port 1025
"""

from twisted.protocols import basic
import uuid
import datetime
import sys

class MyChat(basic.LineReceiver):

    def __init__(self):
        # Create random client id
        self.client = KafkaClient(hosts="127.0.0.1:9092")
        self.request_topic = self.client.topics['mobile-request']
        self.response_topic = self.client.topics['mobile-response']
        self.producer = self.request_topic.get_producer(linger_ms=1)

        self.client_id = str(uuid.uuid4())

    def connectionMade(self):
        
        #self.producer.start() 
        print "Got new client!: " + self.client_id
        
        self.factory.count=self.factory.count+1
        print str(self.factory.count),self

        # Add client to the table (dictionary)
        self.factory.client_map[self.client_id]=self 
        print self.factory.client_map


    def connectionLost(self, reason):
        print "Lost a client!"
        self.factory.count=self.factory.count-1
        print str(self.factory.count),self

        # Remove client form the table (dictionary)
        #self.producer.stop() 
        del self.client
        del self.request_topic
        del self.producer
        self.factory.client_map.pop(self.client_id)


    def lineReceived(self, line):
        print "received", repr(line)

        reactor.callInThread(self.produce_message,self.producer,self.client_id, line)

 
    def produce_message(self,producer,client_id,data):

        message_time=str(datetime.datetime.now())
        dispatcher_id = uuid.uuid4().hex
        message_id = uuid.uuid4().hex
        message=message_time+"|"+message_id+"|"+client_id+"|"+dispatcher_id+"|"+data
        print message
        producer.start()
        producer.produce(message)
        producer.stop()



    def message(self, message):
        self.transport.write(message + '\n')


from twisted.internet import protocol
from twisted.application import service, internet
from twisted.internet import reactor,threads

factory = protocol.ServerFactory()
factory.protocol = MyChat
factory.clients = []
factory.count = 0
factory.client_map = dict()



from pykafka import KafkaClient
from pykafka.balancedconsumer import BalancedConsumer, OffsetType



    
def consume_message_bot(client_map):
    print "In consume_message_bot:", client_map

    client = KafkaClient(hosts="127.0.0.1:9092")
    response_topic = client.topics['mobile-response']

    consumer = response_topic.get_simple_consumer(
        consumer_group="mobileresponegroup",
        auto_commit_enable = True,
        #reset_offset_on_start=True
        auto_offset_reset=OffsetType.LATEST,
    )



    for message in consumer:
         print client_map
         if message is not None:
             print "------>Raw message ["+str(message.offset)+"]: " + message.value
             request_message = message.value.split("|")
             if len(request_message) >=4:
                 try:
                     print "Request: ",request_message
                     # Send data back to chat server
                     clientid = request_message[2] 
                     print clientid

                     data="|".join(request_message[4:])
                     print "Data in consumer: ",data
		     threads.blockingCallFromThread(reactor,client_map[clientid].message,data)
                     
                     consumer.commit_offsets()
                 except:
                     print sys.exc_info()[0]
                     consumer.commit_offsets()
                     pass


# Create Producer and Consumer here



from twisted.internet import protocol
from twisted.application import service, internet
from twisted.internet import reactor,threads

factory = protocol.ServerFactory()
factory.protocol = MyChat
factory.clients = []
factory.count = 0
factory.client_map = dict()
#factory.producer = producer

reactor.suggestThreadPoolSize(30)
reactor.callInThread(consume_message_bot, factory.client_map)



#reactor.callInThread(aSillyBlockMethod, factory.client_map)


application = service.Application("chatserver")
internet.TCPServer(1025, factory).setServiceParent(application)
