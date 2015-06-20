__author__ = 'Zakaria'

from source import Source
from destination import Destination
from random import randint
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


#Test run
# from processor import Processor
if __name__ == '__main__':
    processor = Processor('127.0.0.1', 12345)
    processor.add_neighbour(('127.0.0.1', 12346))
    processor.run_server()