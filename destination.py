__author__ = 'Zakaria'

import node
import asyncio
import pickle
import json
from packet import Packet


#destination or sink or last node of the network
class Destination(node.Node):

    # def run_server(self):
    #     loop = asyncio.get_event_loop()
    #     job = asyncio.start_server(self.handle_packet, self.host, self.port, loop=loop)
    #     server = loop.run_until_complete(job)
    #
    #     print('MSG: Serving on {}'.format(server.sockets[0].getsockname()))
    #     try:
    #         loop.run_forever()
    #     except KeyboardInterrupt:
    #         pass
    #
    #     # Close the server
    #     server.close()
    #     loop.run_until_complete(server.wait_closed())
    #     loop.close()
    #
    # @asyncio.coroutine
    # def handle_packet(self, reader, writer):
    #     data = yield from reader.read()
    #     packet = pickle.loads(data)
    #     # sender = writer.get_extra_info('peername')
    #     sender = packet.sender
    #     if packet:
    #         self.do(packet, sender)
    #
    #     # print("Send: %r" % message)
    #     # writer.write(data)
    #     # yield from writer.drain()
    #
    #     # print("MSG: Close receiving socket")
    #     writer.close()

    def do(self, packet):
        """

        :param packet:
        :param sender:
        """
        print(packet.sender + ' >| ' + packet.type + str(len(packet.data)) + ' |> ' + self.name)
        # print(packet.type + str(len(packet.data)))
        for p in packet.data:
            print(packet.type + ' : ' + json.dumps(p))
        print("----------------------------------------")


#Test run
if __name__ == '__main__':
    dest = Destination('destination', '127.0.0.1', 12350)
    dest.run_server()
