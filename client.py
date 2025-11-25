import socket
import sys
from msg import Message
from database import DataBase

def get_switch_info(clientName):
    db = DataBase(clientName)
    if db.vizinhos:
        switch_host = db.vizinhos[0]
        switch_port = 40331 # Porta Receiver dos Routers
        return switch_host, switch_port
    else:
        print("No neighbors found in the database.")
        return None, None

def get_available_streams(switch_host, switch_port, clientName):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((switch_host, switch_port))

            #Enviar pedido de streams disponiveis
            request_message = Message(Message.WHAT_VIDEO, None, clientName,"")
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
        print("Error connecting to switch:", e)
        return []

def requestStream(switch_host, switch_port, client_name, stream_number):
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(5)
            s.connect((switch_host, switch_port))

            #Enviar pedido de stream
            request_message = Message(Message.STREAM_PLS, stream_number, client_name)
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

    #Verificar qual switch correspondente
    switch_host, switch_port = get_switch_info(clientName)
    if not switch_host:
        print(f"No switch information found for client {clientName}")
        sys.exit(1)

    print("Connecting to switch at {}:{}".format(switch_host, switch_port))

    print("Trying to receive streams available from switch...")
    streams = get_available_streams(switch_host, switch_port, clientName)

    if not streams:
        print("No streams received from switch.")
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

            choice_num = int(stream_choice)
            print(f"Requesting stream: {choice_num}")
            response = requestStream(switch_host, switch_port, clientName, choice_num)
            print("Response from switch:", response)
        except KeyboardInterrupt:
            print("\nExiting.")
            break
        except Exception as e:
            print("An error occurred:", e)

if __name__ == '__main__':
    main()


