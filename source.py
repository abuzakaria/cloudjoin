__author__ = 'Zakaria'
import node
import asyncio
import random


#source or first node of the network
class Source(node.Node):

    neighbours = []

    def add_neighbour(self, neighbour):
        self.neighbours.append(neighbour)

    def remove_neighbour(self, neighbour):
        self.neighbours.remove(neighbour)

    def count_neighbours(self):
        return len(self.neighbours)

    def send(self, payload, receiver_host, receiver_port):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tcp_echo_client(payload, receiver_host, receiver_port, loop))
        loop.close()

    @asyncio.coroutine
    def tcp_echo_client(self, message, receiver_host, receiver_port, loop):
        reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=loop)

        print('SND: %r' % message)
        writer.write(message.encode())
        yield from writer.drain()

        # data = yield from reader.read(self.window_size)
        # print('Received: %r' % data.decode())

        print('MSG: Close the socket')
        writer.close()


#Test run
# src = Source()
# src.add_neighbour(('127.0.0.1', 12345))
# n = src.neighbours[random.randint(0, src.count_neighbours()-1)]
# src.send("test", n[0], n[1])