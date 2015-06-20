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
        # loop.close()

    @asyncio.coroutine
    def tcp_echo_client(self, message, receiver_host, receiver_port, loop):
        reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=loop)

        print(str(self.port) + ' >| ' + message + ' |> ' + str(receiver_port))
        writer.write(message.encode())
        yield from writer.drain()

        # data = yield from reader.read(self.window_size)
        # print('Received: %r' % data.decode())

        print('MSG: Close sending socket')
        writer.close()


#Test run
if __name__ == '__main__':
    src = Source('127.0.0.1', '12344')
    hst = '127.0.0.1'
    for prt in range(12345, 12350):
        src.add_neighbour((hst, prt))
        src.send("ok2", hst, prt)