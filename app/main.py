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

        request_api_bytes = data[6:8]
        request_api = struct.unpack(">h",request_api_bytes)[0]

        if request_api > 4 or request_api < 0:
            error_code = struct.pack(">h",35)
        else:
            error_code = struct.pack(">h",0)

        api_key_recieved = struct.unpack(">h",data[4,6])[0]
        api_version_recieved = struct.unpack(">h",data[6:8])[0]
        correlation_id = struct.unpack(">i", data[8:12])[0]
        print(f"Parsed api_key={api_key_recieved}, api_version={api_version_recieved}, correlation_id={correlation_id}")

        # Build response

        num_api_keys = struct.pack(">b",2)

        api_key = struct.pack(">h",18)
        min_version = struct.pack(">h",0)
        max_version = struct.pack(">h",0)

        api_keys = api_key + min_version + max_version

        tag_buffer = b'\0x00'

        throttle_time_ms = struct.pack(">i",0)

        response_body = {
            error_code +
            num_api_keys +
            api_keys +
            tag_buffer +
            throttle_time_ms +
            tag_buffer
        }
        
        response_payload = response_correlation_id + response_body

        message_size = struct.pack(">i", len(response_payload))
        response_correlation_id = struct.pack(">i", correlation_id)
        response = message_size + response_payload

        # Send response and close
        conn.sendall(response)
        print(f"Sent response with correlation_id={correlation_id}")
        conn.close()

if __name__ == "__main__":
    main()
