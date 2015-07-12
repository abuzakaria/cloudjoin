__author__ = 'Zakaria'

from source import Source
from destination import Destination
from region import Region
import asyncio


#cores or joining nodes.
class Processor(Source, Destination):
    def __init__(self, name=None, host=None, port=None, window_size=100):
        if host:
            self.host = host
        if port:
            self.port = port
        if name:
            self.name = name
        self.LR = Region('S', window_size)
        self.RR = Region('R', window_size)

    def set_window_size(self, n):
        self.LR.change_window_size(n)
        self.RR.change_window_size(n)

    def do(self, packet, sender):
        # print(str(sender) + ' >| ' + packet.type + str(packet.data[0]) + ' |> ' + str(self.port))
        join_result = None
        if packet.type == 'R':
            if packet.store is True:
                self.RR.store(packet)      # store r
            join_result = self.LR.process(packet)

        elif packet.type == 'S':
            if packet.store is True:
                self.LR.store(packet)      # store s
            join_result = self.RR.process(packet)

        receiver = self.neighbours[0]       # for now this is the only destination neighbour
        if len(join_result.data) > 0:
            self.send(join_result, receiver[0], receiver[1])

    def send(self, payload, receiver_host, receiver_port):
        payload.sender = self.name
        loop = asyncio.get_event_loop()
        asyncio.async(self.tcp_client(payload, receiver_host, receiver_port, loop))

# if __name__ == '__main__':
#     hst = '127.0.0.1'
#     for prt in range(12345, 12349):
#         processor = Processor(hst, prt)
#         processor.add_neighbour((hst, 12350))
#         loop = asyncio.get_event_loop()
#         t = Thread(target=processor.run_server, args=(loop,))
#         t.start()

#
# from processor import Processor
# p = Processor('127.0.0.1', )
# p.add_neighbour(('127.0.0.1', 12350))
# p.run_server()