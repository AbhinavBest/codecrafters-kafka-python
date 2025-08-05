import socket
import struct
import threading

def encode_unsigned_varint(value):
    out = b''
    while True:
        bits = value & 0x7F
        value >>= 7
        if value:
            out += struct.pack("B", bits | 0x80)
        else:
            out += struct.pack("B", bits)
            break
    return out

#====
def handleClient(conn):
    supported_api_versions = [
        (18,0,4),
        (75,0,0)
    ]

    try:
        while True:
            header = conn.recv(4)
            if not header:
                print("Client closed connection")
                break
            if len(header) < 4:
                print("Incomplete request header")
                break

            msg_len = struct.unpack(">i", header)[0]
            data = b''
            while len(data) < msg_len:
                chunk = conn.recv(msg_len - len(data))
                if not chunk:
                    print("Client closed connection mid-request")
                    return
                data += chunk

            print(f"Received request of length {len(data)}")

            api_key = struct.unpack(">h", data[0:2])[0]
            api_version = struct.unpack(">h", data[2:4])[0]
            correlation_id = struct.unpack(">i", data[4:8])[0]
            print(f"Parsed api_key={api_key}, api_version={api_version}, correlation_id={correlation_id}")

            # error_code
            if api_version > 4 or api_version < 0:
                error_code = struct.pack(">h", 35)
            else:
                error_code = struct.pack(">h", 0)

            # Build response
            response_correlation_id = struct.pack(">i", correlation_id)

            body = error_code
            body += encode_unsigned_varint(len(supported_api_versions)+1)
            for api_k, min_v, max_v in supported_api_versions:
                body += struct.pack(">hhh", api_k, min_v, max_v)
                # tagged_fields for this entry (empty)
                body += encode_unsigned_varint(0)
            body += struct.pack(">i", 0)  # throttle_time_ms=0
            body += encode_unsigned_varint(0)  # final tagged_fields (empty)

            response_payload = response_correlation_id + body
            response = struct.pack(">i", len(response_payload)) + response_payload

            conn.sendall(response)
            print(f"Sent response with correlation_id={correlation_id}")

    finally:
        conn.close()
        print("Closed connection")

def main():
    print("Starting broker on port 9092")
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    while True:
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")
        threading.Thread(target=handleClient, args=(conn,), daemon=True).start()

if __name__ == "__main__":
    main()
