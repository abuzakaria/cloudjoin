__author__ = 'Zakaria'
import node
import asyncio
import time
import pickle
from packet import Packet


#source or first node of the network
class Source(node.Node):
    lines = list()
    list_of_nodes = []

    def add_node_to_list(self, neighbour):
        """

        :param neighbour:
        """
        if neighbour not in self.list_of_nodes:
            self.list_of_nodes.append(neighbour)

    def remove_node_from_list(self, neighbour):
        """

        :param neighbour:
        """
        self.list_of_nodes.remove(neighbour)

    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """
        for (host, port) in self.list_of_nodes:
            if int(packet.data[0]) % 5 == port % 5:
                print(host + ' ' + str(port))
                packet.store = True
            else:
                packet.store = False
            self.send(packet, host, port)

    def do(self, packet):
        print(packet.type + ' from ' + packet.sender['name'])
        if packet.type == 'H':
            self.add_node_to_list((packet.sender['host'], packet.sender['port']))
        print(self.list_of_nodes)

    # def send(self, message, receiver_host, receiver_port):
    #         """
    #
    #         :param message:
    #         :param receiver_host:
    #         :param receiver_port:
    #         """
    #         message.sender = self.name
    #         loop = asyncio.get_event_loop()
    #         loop.run_until_complete(self.tcp_client(message, receiver_host, receiver_port, loop))
            # loop.close()


    # @asyncio.coroutine
    # def tcp_client(self, message, receiver_host, receiver_port, loop):
    #     reader, writer = yield from asyncio.open_connection(receiver_host, receiver_port, loop=loop)
    #
    #     print(self.name + ' >| ' + message.type + str(message.data[0]) + ' |> ' + str(receiver_port))
    #     writer.write(pickle.dumps(message))
    #     yield from writer.drain()
    #
    #     # data = yield from reader.read(self.window_size)
    #     # print('Received: %r' % data.decode())
    #
    #     print('----------------')
    #     writer.close()

###########################################################


lines = list()


def get_data():
    global lines
    with open("data.txt") as f:
        lines = f.read().splitlines()

#Test run
if __name__ == '__main__':
    src = Source('source', '127.0.0.1', '12344')
    hst = '127.0.0.1'
    # add processor nodes as neighbors
    # for prt in range(12345, 12350):
    #     src.add_neighbour((hst, prt))

    get_data()
    l = len(lines)
    for i in range(0, l, 3):    # loop through 3 lines at a time
        pack = Packet(lines[i])
        pack.append_data(lines[i+1])
        pack.append_data(lines[i+2])

        # src.distribute(pack)
        src.run_server()
        # src.send(pack, '127.0.0.1', 12350)
        time.sleep(1)