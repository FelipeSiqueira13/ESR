import json
import sys

class DataBase():

    def __init__(self, name):
        #self.vizinhos = {}

        with open('config.json', 'r') as file:
            ip_config = json.load(file)

        self.vizinhos = ip_config.get(name, [])
        self.vizinhos_inicializados = {viz: 0 for viz in self.vizinhos}

        self.available_streams = []
        self.active_streams_table = {}
        self.streams_origin_table = {}

        #Tabela de streams ativas
        #--|    | s1 | s2 | s3 |
        #--| v1 | 1  | 1  | 0  |
        #--| v2 | 0  | 0  | 1  |
        #--| v3 | 0  | 0  | 0  |
        #--uma tabela para cada dispositivo (Onode)

        # uma entrada de tabela para cada vizinho
        for viz in self.vizinhos:
            self.active_streams_table[viz] = {}


        # print(self.vizinhos)

    #-------------------------------------------------------------
    #-------------FUNCTIONS FOR ACTIVE STREAMS TABLE--------------
    #-------------------------------------------------------------

    # será necessário fornecer tmb a origem (vizinho) da stream?
    def addStream(self, stream_id):
        if self.active_streams_table not in self.available_streams:
            self.available_streams.append(stream_id)
            #excluir provavelmente o vizinho de onde vem a stream!!
            for viz in self.vizinhos:
                self.active_streams_table[viz][stream_id] = 0
            print("Added stream {stream_id}.\n")
        

    def activateStream(self, viz, stream_id):
        if stream_id in self.available_streams and viz in self.active_streams_table:
            self.active_streams_table[viz][stream_id] = 1
            print("Stream {stream_id} activated for neighbour {viz}.\n")
        else:
            print("An error has occurred.\nStream {stream_id} can not be activated for neighbour {viz}.\n")


    def deactivateStream(self, viz, stream_id):
        if stream_id in self.available_streams and viz in self.active_streams_table:
            self.active_streams_table[viz][stream_id] = 0
            print("Stream {stream_id} deactivated for neighbour {viz}.\n")
        else:
            print("An error has occurred.\nStream {stream_id} can not be deactivated for neighbour {viz}.\n")

    
    def printActiveStreamsTable(self):
        print("         " + " ".join(f"{s:>4}" for s in self.available_streams))
        for v in self.vizinhos:
            valores = " ".join(f"{self.active_streams_table[v][s]:>4}" for s in self.available_streams)
            print(f"{v:>4} {valores}")

    
    #-------------------------------------------------------------
    #-------------FUNCTIONS FOR STREAM ORIGINS TABLE--------------
    #-------------------------------------------------------------

    #def add_stream_origin


    #-------------------------------------------------------------
    #------------FUNCTIONS FOR NEIGHBOURS IN OVERLAY--------------
    #-------------------------------------------------------------

    def inicializaVizinho(self, viz):
        if viz in self.vizinhos:
            self.vizinhos_inicializados[viz] = 1
            print("Neighbour {viz} initialized.\n")
        else:
            print("Neighbour {viz} could not be initialized.\n")

    
    def removeVizinho(self, viz):
        if viz in self.vizinhos:
            self.vizinhos_inicializados[viz] = 0
            print("Neighbour {viz} removed.\n")
        else:
            print("Neighbour {viz} could not be removed.\n")
    
    # para debugging
    def printVizinhosInicializados(self):
        for viz, status in self.vizinhos_inicializados.items():
            print(f"{viz}: {status}")



if __name__ == '__main__':
    db = DataBase(sys.argv[1])
    

    # Para testar
    db.printActiveStreamsTable()
    db.addStream("s1")
    db.addStream("s2")
    db.addStream("s3")

    db.printActiveStreamsTable()

    db.activateStream("10.0.4.21", "s1")
    db.activateStream("10.0.4.20", "s2")
    db.activateStream("10.0.4.20", "s3")

    db.printActiveStreamsTable()