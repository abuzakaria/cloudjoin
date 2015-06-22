__author__ = 'Zakaria'
import node
import asyncio
import time
import pickle
from packet import Packet


#source or first node of the network
class Source(node.Node):

    neighbours = []

    def add_neighbour(self, neighbour):
        self.neighbours.append(neighbour)

    def remove_neighbour(self, neighbour):
        self.neighbours.remove(neighbour)

    def count_neighbours(self):
        return len(self.neighbours)

    def send(self, message, receiver_host, receiver_port):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(self.tcp_echo_client(message, receiver_host, receiver_port, loop))
        # loop.close()

    @asyncio.coroutine
    def tcp_echo_client(self, message, receiver_host, receiver_port, loop):
        reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=loop)

        print(str(self.port) + ' >| ' + message.header + str(message.data[0]) + ' |> ' + str(receiver_port))
        writer.write(pickle.dumps(message))
        yield from writer.drain()

        # data = yield from reader.read(self.window_size)
        # print('Received: %r' % data.decode())

        print('----------------')
        writer.close()

###########################################################


lines = list()


def get_data():
    global lines
    with open("data.txt") as f:
        lines = f.read().splitlines()

#Test run
if __name__ == '__main__':
    src = Source('127.0.0.1', '12344')
    hst = '127.0.0.1'
    # add processor nodes as neighbors
    for prt in range(12345, 12350):
        src.add_neighbour((hst, prt))

    get_data()
    l = len(lines)
    for i in range(0, l, 3):    # loop through 3 lines at a time
        pack = Packet(lines[i])
        pack.append_data(lines[i+1])
        pack.append_data(lines[i+2])

        # src.send(pack, hst, 12350)
        for prt in range(12345, 12350):
            src.send(pack, hst, prt)
        time.sleep(2)