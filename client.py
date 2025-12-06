import socket
import sys, json
from msg import Message
from database import DataBase
from onode import send_message

def get_node_info(clientName):
    db = DataBase(clientName)
    if db.vizinhos:
        node_host = db.vizinhos[0]
        node_port = 40331 # Porta Receiver dos Routers
        return node_host, node_port
    else:
        print("No neighbors found in the database.")
        return None, None
    
def get_client_ip(clientName):
    with open('config.json', 'r') as file:
        ip_config = json.load(file)
    
    client_data = ip_config.get(clientName, {})
    
    if client_data:
        return list(client_data.keys())[0]
    else:
        return None


def _send_buffer(sock: socket.socket, payload: bytes):
    view = memoryview(payload)
    total_sent = 0
    while total_sent < len(view):
        sent = sock.send(view[total_sent:])
        if sent == 0:
            raise ConnectionError("Socket connection broken")
        total_sent += sent

def send_tcp_request(node_host, node_port, msg: Message):
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.settimeout(5)

        s.connect((node_host, node_port))

        _send_buffer(s, msg.serialize() + b'\n')

        buffer = b''
        while True:
            chunk = s.recv(4096)
            if not chunk:
                break
            buffer += chunk

            if b'\n' in buffer:
                data, _ = buffer.split(b'\n', 1)
                s.close()
                return Message.deserialize(data)
            
        s.close()
        print("Connection closed without receiving complete response")
        return None
    
    except socket.timeout:
        print("Timeout waiting for response from node.")
        return None
    except Exception as e:
        print(f"Error in TCP request: {e}")
        return None


def get_available_streams(node_host, node_port, clientName):
    try:
        source = get_client_ip(clientName)
        if not source:
            print("Client IP not found in config.")
            return []

        msg = Message(Message.STREAMS_AVAILABLE, source, "")
        response_message = send_tcp_request(node_host, node_port, msg)
        if not response_message:
            return []

        streams_raw = response_message.data.strip()
        if not streams_raw or streams_raw == "No streams available":
            return []
        return [stream.strip() for stream in streams_raw.split(",") if stream.strip()]
    except Exception as e:
        print("Error retrieving streams:", e)
        return []


def requestStream(node_host, node_port, client_name, stream_number):
    try:
        source = get_client_ip(client_name)
        if not source:
            return "Client IP not found."

        msg = Message(Message.STREAM_REQUEST, source, stream_number)
        response_message = send_tcp_request(node_host, node_port, msg)
        if not response_message:
            return "No response from node."
        return response_message.data
    except Exception as e:
        print("Error requesting stream:", e)
        return "Request failed."

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <clientName>")
        sys.exit(1)

    clientName = sys.argv[1]
    print(f"Client {clientName} initialized")

    #Verificar qual node correspondente
    node_host, node_port = get_node_info(clientName)
    if not node_host:
        print(f"No node information found for client {clientName}")
        sys.exit(1)

    print("Connecting to node at {}:{}".format(node_host, node_port))

    print("Trying to receive streams available from node...")
    streams = get_available_streams(node_host, node_port, clientName)

    if not streams:
        print("No streams received from node.")
        sys.exit(1)

    print("\nAvailable streams:")
    for stream in streams:
        print(f"Stream {stream}")

    while True:
        try:
            stream_choice = input("Select a stream by name (or 'quit' to exit): ")
            if stream_choice.lower() == 'quit':
                print("Exiting.")
                break

            print(f"Requesting stream: {stream_choice}")
            response = requestStream(node_host, node_port, clientName, stream_choice)
            print("Response from node:", response)
        except KeyboardInterrupt:
            print("\nExiting.")
            break
        except Exception as e:
            print("An error occurred:", e)

if __name__ == '__main__':
    main()


