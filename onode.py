import socket
import sys
import threading
import queue
from BD import DataBase

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

    if len(sys.argv) < 2:
        sys.exit(1)
    db = DataBase(sys.argv[1])

    thread_listen = threading.Thread(target=, args=())
    thread_sender = threading.Thread(target=, args=())

    all_threads = [thread_listen, thread_sender]

    for t in all_threads:
        t.daemon = True
    
    for t in all_threads:
        t.start()

    for t in all_threads:
        t.join()

    


if __name__ == '__main__':
    main()
