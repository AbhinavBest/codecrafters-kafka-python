import socket
import struct

def main():
    print("Starting plain-socket Kafka-like broker on port 9092...")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")

        data = conn.recv(1024)
        print(f"Received data of length {len(data)}")

        header_offset = 4 + 2 + 2

        request_api_bytes = data[6:8]
        request_api = struct.unpack(">h", request_api_bytes)[0]
        print(f"Parsed request_api_version: {request_api}")

        if request_api > 4 or request_api < 0:
            error_code = struct.pack(">h", 35)
            print("Unsupported version, responding with error_code=35")
        else:
            error_code = struct.pack(">h", 0)
            print("Supported version, responding with error_code=0")

        api_versions_count = struct.pack(">i", 1)
        api_key = struct.pack(">h", 18)
        min_version = struct.pack(">h", 0)
        max_version = struct.pack(">h", 4)

        tag_buffer = b'\x00'  # add empty TAG_BUFFER

        response_body = (
            error_code +
            api_versions_count +
            api_key + min_version + max_version +
            tag_buffer
        )

        correlation_id_bytes = data[header_offset:header_offset+4]
        correlation_id = struct.unpack(">i", correlation_id_bytes)[0]
        print(f"Parsed correlation_id: {correlation_id}")
        response_correlation_id = struct.pack(">i", correlation_id)

        total_length = len(response_correlation_id + response_body)
        message_size = struct.pack(">i", total_length)

        response = message_size + response_correlation_id + response_body

        conn.sendall(response)
        print(f"Sent response with correlation_id={correlation_id}")
        conn.close()

if __name__ == "__main__":
    main()
