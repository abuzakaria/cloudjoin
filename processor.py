__author__ = 'Zakaria'

from source import Source
from destination import Destination
from packet import Packet
from left_region import LeftRegion
from right_region import RightRegion
import asyncio


#cores or joining nodes.
class Processor(Source, Destination):
    def __init__(self, host=None, port=None, window_size=100):
        if host:
            self.host = host
        if port:
            self.port = port
        self.window_size = window_size
        self.LR = LeftRegion(window_size)
        self.RR = RightRegion(window_size)

    def set_window_size(self, n):
        self.window_size = n

    def do(self, message, sender):
        print(str(sender) + '>' + str(message) + '>' + str(self.port))
        if message.header == 'R':
            self.RR.store_r(message)
            self.LR.process_r(message)
        elif message.header == 'S':
            self.LR.store_s(message)
            self.RR.process_s(message)

        # receiver = self.neighbours[0]       # for now this is the destination neighbour
        # self.send(message, receiver[0], receiver[1])

    def send(self, payload, receiver_host, receiver_port):
        loop = asyncio.get_event_loop()
        asyncio.async(self.tcp_echo_client(payload, receiver_host, receiver_port, loop))

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