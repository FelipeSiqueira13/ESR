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


def get_available_streams(node_host, node_port, clientName):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((node_host, node_port))

            source = get_client_ip(clientName)

            #Enviar pedido de streams disponiveis
            request_message = Message(Message.STREAMS_AVAILABLE, source, "")
            s.send(request_message.encode())

            #Receber resposta com streams disponiveis
            response_data = s.recv(1024)
            response_message = Message.decode(response_data)

            try:
                streams = response_message.data.split(",")
                streams = [stream.strip() for stream in streams if stream.strip()]
                return streams
            except Exception as e:
                print("Error parsing streams from response:", e)
                return []
    except Exception as e:
        print("Error connecting to node:", e)
        return []

def requestStream(node_host, node_port, client_name, stream_number):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((node_host, node_port))

            source = get_client_ip(client_name)

            #Enviar pedido de stream
            request_message = Message(Message.STREAM_REQUEST, source, stream_number)
            s.send(request_message.encode())

            #Receber resposta
            response_data = s.recv(1024)
            response_message = Message.decode(response_data)
            return response_message.data
    except Exception as e:
        print("Error requesting stream:", e)
        return []

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <clientName>")
        sys.exit(1)

    clientName = sys.argv[1]
    print(f"Client {clientName} inicialized")

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
            stream_choice = input("Select a stream by number (or 'quit' to exit): ")
            if stream_choice.lower() == 'quit':
                print("Exiting.")
                break

            if not stream_choice.isdigit() or int(stream_choice) < 1 or int(stream_choice) > len(streams):
                print("Invalid choice. Please try again.")
                continue

            choice_num = stream_choice
            print(f"Requesting stream: {choice_num}")
            response = requestStream(node_host, node_port, clientName, choice_num)
            print("Response from node:", response)
        except KeyboardInterrupt:
            print("\nExiting.")
            break
        except Exception as e:
            print("An error occurred:", e)

if __name__ == '__main__':
    main()


