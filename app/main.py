import socket
import struct
import threading

def handleClient(conn):
    try:
        while True:
            header = conn.recv(4)
            if not header:
                print("Client closed connection")
                break
            if len(header) < 4:
                print("Incomplete connection request")
                break
            msg_len = struct.unpack('>i',header)[0]
            data = b''
            while len(data) < msg_len:
                chunk = conn.recv(msg_len - len(data))
                if not chunk:
                    print("Client closed connection while sending request")
                    return
                data += chunk
            print(f"Received full request of length {len(data)}")

            api_key_recieved = struct.unpack(">h",data[0:2])[0]
            api_version_recieved = struct.unpack(">h",data[2:4])[0]
            correlation_id = struct.unpack(">i", data[4:8])[0]
            print(f"Parsed api_key={api_key_recieved}, api_version={api_version_recieved}, correlation_id={correlation_id}")

            if api_version_recieved > 4 or api_version_recieved < 0:
                error_code = struct.pack(">h",35)
            else:
                error_code = struct.pack(">h",0)
            
            # Build response

            response_correlation_id = struct.pack(">i", correlation_id)
            
            def pack_api_key_entry(api_key, min_version, max_version):
                return (
                    struct.pack(">h", api_key) +
                    struct.pack(">h", min_version) +
                    struct.pack(">h", max_version)
                )
            supported_api_versions = [
                (18,0,4),
                (75,0,0)
            ]

            num_api_keys = b'\x02'
            
            api_keys = b''.join(
                pack_api_key_entry(api_k,min_v,max_v)
                for api_k,min_v,max_v in supported_api_versions
            )

            tag_buffer = b'\x00'

            throttle_time_ms = struct.pack(">i",0)

            response_body = (
                error_code +
                num_api_keys +
                api_keys +
                throttle_time_ms +
                tag_buffer
            )
            

            response_payload = response_correlation_id + response_body

            message_size = struct.pack(">i", len(response_payload))
            response = message_size + response_payload

            # Send response and close
            conn.sendall(response)
            print(f"Sent response with correlation_id={correlation_id}")
    finally:
        conn.close()
        print("Closed Connection")

def main():
    print("Starting plain-socket Kafka-like broker on port 9092...")

    # Create TCP server socket
    server = socket.create_server(("localhost", 9092), reuse_port=True)

    while True:
        # Wait for client
        conn, addr = server.accept()
        print(f"Accepted connection from {addr}")
        threading.Thread(target=handleClient, args=(conn,)).start()

if __name__ == "__main__":
    main()
