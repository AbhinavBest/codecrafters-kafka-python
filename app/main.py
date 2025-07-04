from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor
import struct

class KafkaProtocol(Protocol):
    def dataReceived(self, data: bytes):
        print("Received data from client")
        message_size = struct.pack(">i", 4)
        correlation_id = struct.pack(">i", 7)
        response = message_size + correlation_id
        self.transport.write(response)
        print("Sent response with correlation_id=7")

class KafkaFactory(Factory):
    def buildProtocol(self, addr):
        return KafkaProtocol()

def main():
    print("Starting Twisted Kafka-like broker on port 9092...")
    reactor.listenTCP(9092, KafkaFactory(), interface="localhost")
    reactor.run()

if __name__ == "__main__":
    main()
