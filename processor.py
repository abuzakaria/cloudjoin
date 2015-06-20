__author__ = 'Zakaria'

from source import Source
from destination import Destination
from random import randint
from threading import Thread
import asyncio


#cores or joining nodes.
class Processor(Source, Destination):

    def do(self, message, sender):
        print(str(sender) + '>' + message + '>' + str(self.port))
        receiver = self.neighbours[randint(0, self.count_neighbours()-1)]
        self.send(message, receiver[0], receiver[1])

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