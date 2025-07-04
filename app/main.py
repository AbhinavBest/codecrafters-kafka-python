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

        header_offset = 4 + 2 + 2

        request_api_bytes = data[6:8]
        request_api = struct.unpack(">h",request_api_bytes)[0]

        if request_api > 4 or request_api < 0:
            error_code = struct.pack(">h",35)
        else:
            error_code = struct.pack(">h",0)

        api_versions_count = struct.pack(">i", 1)  # one entry
        api_key = struct.pack(">h", 18)  # API_VERSIONS
        min_version = struct.pack(">h", 0)
        max_version = struct.pack(">h", 4)

        response_body = error_code + api_versions_count + api_key + min_version + max_version

        correlation_id_bytes = data[header_offset:header_offset+4]
        correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
        print(f"Parsed correlation_id: {correlation_id}")
        response_correlation_id = struct.pack(">i", correlation_id)
        
        # Build response
        total_length = len(response_correlation_id + response_body)
        message_size = struct.pack(">i", total_length)
        response = message_size + response_correlation_id + response_body

        # Send response and close
        conn.sendall(response)
        print(f"Sent response with correlation_id={correlation_id}")
        conn.close()

if __name__ == "__main__":
    main()
