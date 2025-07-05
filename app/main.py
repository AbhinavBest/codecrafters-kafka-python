import socket
import struct

def main():
    print("Starting ApiVersions (v4) broker on port 9092...")

    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")

        data = conn.recv(1024)
        print(f"Received data of length {len(data)}")

        # Parse request header v2
        api_key = struct.unpack(">h", data[4:6])[0]
        api_version = struct.unpack(">h", data[6:8])[0]
        correlation_id = struct.unpack(">i", data[8:12])[0]
        print(f"Parsed api_key={api_key}, api_version={api_version}, correlation_id={correlation_id}")

        # === Build response ===


        # Response header v0
        response_correlation_id = struct.pack(">i", correlation_id)

        # Response body
        error_code = struct.pack(">h", 0)

        num_api_keys = struct.pack(">b", 1)  # INT8, value 1

        api_key = struct.pack(">h", 18)  # APIVersions
        min_version = struct.pack(">h", 0)
        max_version = struct.pack(">h", 4)

        api_keys_entry = api_key + min_version + max_version

        # After array: TAG_BUFFER, here: varuint=0
        tag_buffer_after_api_keys = b'\x00'

        throttle_time_ms = struct.pack(">i", 0)

        # After throttle_time_ms: another TAG_BUFFER
        tag_buffer_after_throttle = b'\x00'

        response_body = (
            error_code +
            num_api_keys +
            api_keys_entry +
            tag_buffer_after_api_keys +
            throttle_time_ms +
            tag_buffer_after_throttle
        )

        payload = response_correlation_id + response_body
        message_size = struct.pack(">i", len(payload))

        response = message_size + payload

        print(f"Response hex: {response.hex()}")
        print(f"Total length={len(response)}")

        conn.sendall(response)
        print(f"Sent ApiVersions response (total length={len(response)})")
        conn.close()

if __name__ == "__main__":
    main()
