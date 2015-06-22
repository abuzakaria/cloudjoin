__author__ = 'Zakaria'

import node
import asyncio
import pickle
import json
from packet import Packet


#destination or sink or last node of the network
class Destination(node.Node):

    def run_server(self):
        loop = asyncio.get_event_loop()
        job = asyncio.start_server(self.handle_echo, self.host, self.port, loop=loop)
        server = loop.run_until_complete(job)

        print('MSG: Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

    @asyncio.coroutine
    def handle_echo(self, reader, writer):
        data = yield from reader.read()
        packet = pickle.loads(data)
        sender = writer.get_extra_info('peername')
        if packet:
            self.do(packet, sender)

        # print("Send: %r" % message)
        # writer.write(data)
        # yield from writer.drain()

        # print("MSG: Close receiving socket")
        writer.close()

    def do(self, packet, sender):
        # print(str(sender) + ' >| ' + packet.header + str(len(packet.data)) + ' |> ' + str(self.port))
        # print(packet.header + str(len(packet.data)))
        for p in packet.data:
            print(packet.header + ' : ' + json.dumps(p))
        print("----------------------------------------")


#Test run
if __name__ == '__main__':
    dest = Destination('127.0.0.1', 12350)
    dest.run_server()