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

        # Parse request header
        # request header v2: api_key(int16), api_version(int16), correlation_id(int32), client_id(string)

        api_key = struct.unpack(">h", data[4:6])[0]
        api_version = struct.unpack(">h", data[6:8])[0]
        correlation_id = struct.unpack(">i", data[8:12])[0]
        print(f"Parsed api_key={api_key}, api_version={api_version}, correlation_id={correlation_id}")

        # === Build response ===

        # Response header v0
        response_correlation_id = struct.pack(">i", correlation_id)

        # Response body:
        error_code = struct.pack(">h", 0)

        # ApiKeys array: 1 element, key=18, min_version=0, max_version=4
        api_keys_array_len = struct.pack(">i", 1)

        api_key_18 = struct.pack(">h", 18)
        min_version = struct.pack(">h", 0)
        max_version = struct.pack(">h", 4)

        api_keys_entry = api_key_18 + min_version + max_version

        # ThrottleTimeMs
        throttle_time_ms = struct.pack(">i", 0)

        response_body = error_code + api_keys_array_len + api_keys_entry + throttle_time_ms

        # Compute message size (excluding the first 4 bytes)
        payload = response_correlation_id + response_body
        message_size = struct.pack(">i", len(payload))

        response = message_size + payload

        conn.sendall(response)
        print(f"Sent ApiVersions response (len={len(response)})")
        conn.close()

if __name__ == "__main__":
    main()
