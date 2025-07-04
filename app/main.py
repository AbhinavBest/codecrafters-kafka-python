from twisted.internet.protocol import Protocol, Factory
from twisted.internet import reactor
import struct

class KafkaProtocol(Protocol):
    def dataReceived(self, data: bytes):
        print(f"Received data of length {len(data)}")

        header_offset = 4 + 2 + 2
        correlation_id_bytes = data[header_offset:header_offset+4]
        correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
        print(f"Parsed correlation_id: {correlation_id}")
        message_size = struct.pack(">i", 0)
        response_correlation_id = struct.pack(">i",correlation_id)
        response = message_size + response_correlation_id
        self.transport.write(response)
        print(f"Sent response with correlation_id={correlation_id}")

class KafkaFactory(Factory):
    def buildProtocol(self, addr):
        return KafkaProtocol()

def main():
    print("Starting Twisted Kafka-like broker on port 9092...")
    reactor.listenTCP(9092, KafkaFactory(), interface="localhost")
    reactor.run()

if __name__ == "__main__":
    main()
