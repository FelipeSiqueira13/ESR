import socket
import threading
import queue


class Router:
    def __init__(self, router_host, router_port):
        """
        Inicializa o Router que gerencia conexões com vizinhos (outros routers) e clientes
        
        Args:
            router_host: Host onde este router vai escutar
            router_port: Porta onde este router vai escutar
        """
        self.router_host = router_host
        self.router_port = router_port
        
        self.server_socket = None
        self.neighbors = {}  # Vizinhos (outros routers) {id: socket}
        self.clients = {}    # Clientes conectados {id: socket}
        
        self.incoming_queue = queue.Queue()  # Fila de mensagens recebidas
        self.outgoing_queue = queue.Queue()  # Fila de mensagens para enviar
        
        self.neighbor_counter = 0
        self.client_counter = 0
        self.running = True
        self.lock = threading.Lock()
    
    def accept_connections(self):
        """Thread 1: Aceita conexões de vizinhos e clientes"""
        self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.server_socket.bind((self.router_host, self.router_port))
        self.server_socket.listen(5)
        
        print(f"[ROUTER] Escutando em {self.router_host}:{self.router_port}")
        
        while self.running:
            try:
                conn, address = self.server_socket.accept()
                print(f"[ROUTER] Nova conexão de {address}")
                
                # Cria thread para receber dados dessa conexão
                recv_thread = threading.Thread(
                    target=self.receive_from_connection,
                    args=(conn, address)
                )
                recv_thread.daemon = True
                recv_thread.start()
                
            except Exception as e:
                if self.running:
                    print(f"[ROUTER] Erro ao aceitar conexão: {e}")
    
    def receive_from_connection(self, conn, address):
        """Recebe mensagens de uma conexão específica"""
        connection_type = None  # 'neighbor' ou 'client'
        connection_id = None
        
        try:
            while self.running:
                data = conn.recv(1024).decode().strip()
                
                if not data:
                    break
                
                # Primeira mensagem identifica o tipo de conexão
                if connection_type is None:
                    if data.startswith("NEIGHBOR:"):
                        connection_type = "neighbor"
                        connection_id = int(data.split(":")[1])
                        with self.lock:
                            self.neighbors[connection_id] = conn
                        print(f"[ROUTER] Vizinho {connection_id} conectado de {address}")
                        continue
                    else:
                        connection_type = "client"
                        with self.lock:
                            self.client_counter += 1
                            connection_id = self.client_counter
                            self.clients[connection_id] = conn
                        print(f"[ROUTER] Cliente {connection_id} conectado de {address}")
                
                # Coloca mensagem na fila para processar
                message = {
                    'from_type': connection_type,
                    'from_id': connection_id,
                    'from_address': address,
                    'data': data
                }
                self.incoming_queue.put(message)
                print(f"[ROUTER] Recebido de {connection_type} {connection_id}: {data}")
                
        except Exception as e:
            print(f"[ROUTER] Erro ao receber de {address}: {e}")
        finally:
            with self.lock:
                if connection_type == "neighbor" and connection_id in self.neighbors:
                    del self.neighbors[connection_id]
                elif connection_type == "client" and connection_id in self.clients:
                    del self.clients[connection_id]
            conn.close()
            print(f"[ROUTER] Conexão de {address} fechada")
    
    def send_messages(self):
        """Thread 2: Envia mensagens que estão na fila de saída"""
        while self.running:
            try:
                # Aguarda mensagem na fila (timeout para permitir check de running)
                message = self.outgoing_queue.get(timeout=1)
                
                target_type = message['to_type']  # 'neighbor' ou 'client'
                target_id = message['to_id']
                data = message['data']
                
                with self.lock:
                    if target_type == "neighbor" and target_id in self.neighbors:
                        self.neighbors[target_id].send(data.encode())
                        print(f"[ROUTER] Enviado para vizinho {target_id}: {data}")
                    elif target_type == "client" and target_id in self.clients:
                        self.clients[target_id].send(data.encode())
                        print(f"[ROUTER] Enviado para cliente {target_id}: {data}")
                    else:
                        print(f"[ROUTER] Destino {target_type} {target_id} não encontrado")
                        
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ROUTER] Erro ao enviar mensagem: {e}")
    
    def process_messages(self):
        """Código principal: Processa mensagens e gerencia vizinhos"""
        print("\n[ROUTER] Sistema de roteamento iniciado")
        print("[ROUTER] Aguardando mensagens...\n")
        
        while self.running:
            try:
                # Processa mensagens da fila de entrada
                message = self.incoming_queue.get(timeout=1)
                
                from_type = message['from_type']
                from_id = message['from_id']
                data = message['data']
                
                # Lógica de roteamento: encaminha para vizinhos ou clientes
                if from_type == "client":
                    # Cliente enviou → encaminha para vizinhos
                    print(f"[ROUTER] Roteando mensagem do cliente {from_id} para vizinhos")
                    with self.lock:
                        for neighbor_id in self.neighbors.keys():
                            self.outgoing_queue.put({
                                'to_type': 'neighbor',
                                'to_id': neighbor_id,
                                'data': f"[CLIENT {from_id}] {data}"
                            })
                
                elif from_type == "neighbor":
                    # Vizinho enviou → encaminha para clientes
                    print(f"[ROUTER] Roteando mensagem do vizinho {from_id} para clientes")
                    with self.lock:
                        for client_id in self.clients.keys():
                            self.outgoing_queue.put({
                                'to_type': 'client',
                                'to_id': client_id,
                                'data': f"[NEIGHBOR {from_id}] {data}"
                            })
                
            except queue.Empty:
                continue
            except Exception as e:
                print(f"[ROUTER] Erro ao processar mensagem: {e}")
    
    def start(self):
        """Inicia o router com três threads"""
        print(f"\n{'='*50}")
        print(f"Iniciando Router em {self.router_host}:{self.router_port}")
        print(f"{'='*50}\n")
        
        # Thread 1: Aceita conexões (recebe)
        thread_accept = threading.Thread(target=self.accept_connections, daemon=True)
        thread_accept.start()
        
        # Thread 2: Envia mensagens
        thread_send = threading.Thread(target=self.send_messages, daemon=True)
        thread_send.start()
        
        # Código principal: Processa e gerencia
        try:
            self.process_messages()
        except KeyboardInterrupt:
            print("\n[ROUTER] Encerrando...")
            self.running = False
            if self.server_socket:
                self.server_socket.close()


def run_router():
    """Executa o router"""
    ROUTER_HOST = "0.0.0.0"  # Escuta em todas as interfaces
    ROUTER_PORT = 5001       # Porta do router
    
    router = Router(ROUTER_HOST, ROUTER_PORT)
    router.start()


if __name__ == '__main__':
    run_router()
