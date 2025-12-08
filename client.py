import socket
import sys, json
import struct
from msg import Message
from database import DataBase
from onode import send_message
import threading, time
from SimplePacket import SimplePacket

SENDER_PORT = 40332  # porta de receção UDP de dados (MM)

_frame_buffer = {}
_frame_lock = threading.Lock()
_running = True

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

def udp_listener():
    """Escuta frames via UDP (SimplePacket) e armazena por frame_num."""
    udp_sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    udp_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    udp_sock.bind(('', SENDER_PORT))
    print(f"[CLIENT][UDP] listening on 0.0.0.0:{SENDER_PORT}")

    while _running:
        try:
            raw, addr = udp_sock.recvfrom(65535)
            try:
                pkt = SimplePacket.decode(raw)
            except Exception as e:
                print(f"[CLIENT][UDP][DROP] decode: {e}")
                continue

            payload = pkt.get_payload()
            if b'\0' not in payload:
                print("[CLIENT][UDP][DROP] malformed payload (no NUL)")
                continue
            stream_id_b, batch_data = payload.split(b'\0', 1)
            try:
                stream_id = stream_id_b.decode('utf-8')
            except Exception:
                print("[CLIENT][UDP][DROP] bad stream_id bytes")
                continue

            base_frame_num = pkt.get_frame_num()
            
            try:
                # Unpack batch: Count(1B) + [Len(4B) + Frame]*Count
                if len(batch_data) < 1:
                     # Fallback for single frame (old format) or empty
                     # Assuming new format always has count. 
                     # If we want backward compatibility, we check if it looks like a batch.
                     # But let's assume we upgraded the protocol.
                     raise ValueError("Empty batch data")

                count = struct.unpack("!B", batch_data[:1])[0]
                offset = 1
                
                with _frame_lock:
                    buf = _frame_buffer.setdefault(stream_id, {})
                    
                    for i in range(count):
                        if offset + 4 > len(batch_data):
                            break
                        f_len = struct.unpack("!I", batch_data[offset:offset+4])[0]
                        offset += 4
                        if offset + f_len > len(batch_data):
                            break
                        frame_b = batch_data[offset:offset+f_len]
                        offset += f_len
                        
                        current_frame_num = base_frame_num + i
                        buf[current_frame_num] = frame_b
                    
                    # Limita buffer a 200 frames
                    if len(buf) > 200:
                        oldest = min(buf.keys())
                        buf.pop(oldest, None)

            except Exception as e:
                print(f"[CLIENT][UDP][DROP] batch decode error: {e}")
                continue

            # Reduz verbosidade: imprime apenas a cada 60 frames (aprox 2s)
            if base_frame_num % 60 == 0:
                print(f"[CLIENT][UDP][RX] stream={stream_id} frame={base_frame_num} batch_size={count} from={addr[0]}")
        except OSError:
            break
        except Exception as e:
            print(f"[CLIENT][UDP][ERR] {e}")
    udp_sock.close()

def send_stream_stop(node_host, node_port, client_name, stream_number):
    try:
        source = get_client_ip(client_name)
        if not source:
            return
        msg = Message(Message.STREAM_STOP, source, stream_number)
        send_tcp_request(node_host, node_port, msg)
        print(f"[CLIENT] STREAM_STOP sent for {stream_number}")
    except Exception as e:
        print(f"[CLIENT] STREAM_STOP error: {e}")

def send_ping(node_host, node_port, client_name, stream_id, ttl=8):
    source = get_client_ip(client_name)
    if not source:
        return "Client IP not found."
    payload = json.dumps({"stream_id": stream_id, "ttl": ttl, "path": []})
    msg = Message(Message.PING, source, payload)
    send_tcp_request(node_host, node_port, msg)
    return "PING sent."

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <clientName>")
        sys.exit(1)

    clientName = sys.argv[1]
    print(f"Client {clientName} initialized")

    node_host, node_port = get_node_info(clientName)
    if not node_host:
        print(f"No node information found for client {clientName}")
        sys.exit(1)

    # Inicia listener UDP de frames
    t_udp = threading.Thread(target=udp_listener, daemon=True)
    t_udp.start()

    streams = get_available_streams(node_host, node_port, clientName)
    if not streams:
        print("No streams received from node.")
        return

    print("\nAvailable streams:")
    for stream in streams:
        print(f"Stream {stream}")

    current_stream = None
    try:
        while True:
            stream_choice = input("Select a stream by name (or 'quit' to exit, 'ping <stream>' to ping): ")
            if stream_choice.lower() == 'quit':
                break
            if stream_choice.lower().startswith("ping "):
                sid = stream_choice.split(None, 1)[1].strip()
                print(send_ping(node_host, node_port, clientName, sid))
                continue
            print(f"Requesting stream: {stream_choice}")
            response = requestStream(node_host, node_port, clientName, stream_choice)
            print("Response from node:", response)
            if response == "OK":
                current_stream = stream_choice
    except KeyboardInterrupt:
        pass
    finally:
        global _running
        _running = False
        # Envia STREAM_STOP se havia uma stream selecionada
        if current_stream:
            send_stream_stop(node_host, node_port, clientName, current_stream)
        # dá tempo para fechar o UDP
        time.sleep(0.5)

if __name__ == '__main__':
    main()


