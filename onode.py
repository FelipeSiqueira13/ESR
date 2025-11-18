import socket
import sys
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


def main():
    sys.stdout.write(f"\033]0;{socket.gethostname()}\007")
    sys.stdout.flush()
    
    

    """Executa o router"""
    ROUTER_HOST = "127.0.0.1"  # Escuta em localhost
    ROUTER_PORT = 5001       # Porta do router
    
    router = Router(ROUTER_HOST, ROUTER_PORT)
    router.start()


if __name__ == '__main__':
    main()
