import socket
import struct

def main():
    print("Starting plain-socket Kafka-like broker on port 9092...")

    # Create TCP server socket
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        # Wait for client
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")

        # Receive request (can adjust buffer size; 1024 is fine for now)
        data = conn.recv(1024)
        print(f"Received data of length {len(data)}")

        # Parse correlation_id
        header_offset = 4 + 2 + 2
        correlation_id_bytes = data[header_offset:header_offset+4]
        correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
        print(f"Parsed correlation_id: {correlation_id}")

        # Build response
        message_size = struct.pack(">i", 0)  # any value works
        response_correlation_id = struct.pack(">i", correlation_id)
        response = message_size + response_correlation_id

        # Send response
        conn.sendall(response)
        print(f"Sent response with correlation_id={correlation_id}")

        # Close client connection
        conn.close()

if __name__ == "__main__":
    main()
