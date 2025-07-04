import struct
import socket  # noqa: F401


def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # Uncomment this to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    conn, addr = server.accept() # wait for client
    print(f"Accepted connection from {addr}")

    message_id = struct.pack(">i",0)
    correlation_id = struct.pack(">i",7)

    response = message_id + correlation_id

    conn.sendall(response)
    conn.close()

if __name__ == "__main__":
    main()
