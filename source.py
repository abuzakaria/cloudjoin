__author__ = 'Zakaria'
import node
import asyncio
import time
import constants
from packet import Packet


#source or first node of the network
class Source(node.Node):
    lines = list()
    active_nodes = []
    is_membership_protocol_running = False

    def add_node_to_list(self, reporting_node):
        """

        :param reporting_node:
        """
        for row in self.active_nodes:
            if row:
                if reporting_node == row[0]:
                    row[1] = time.time()
                    return

        self.active_nodes.append([reporting_node, time.time()])

    def remove_node_from_list(self, inactive_node):
        """

        :param inactive_node:
        """
        for row in self.active_nodes:
            if inactive_node == row[0]:
                self.active_nodes.remove(row)

    def check_active_nodes(self, interval):
        current_time = time.time()
        for row in self.active_nodes:
            if row[1] + interval < current_time:
                self.remove_node_from_list(row[0])
        self.loop.call_later(interval, self.check_active_nodes, interval)

    def distribute(self, packet):
        """
        distributes packet to different processor nodes
        :param packet: packet to distribute
        """
        for row in self.active_nodes:
            (host, port) = row[0]
            if int(packet.data[0]) % 5 == port % 5:
                print(host + ' ' + str(port))
                packet.store = True
            else:
                packet.store = False
            self.send(packet, host, port)

    def do(self, packet):
        print(packet.type + ' from ' + packet.sender['name'])
        if packet.type == constants.DATATYPE_HEARTBEAT:
            self.add_node_to_list((packet.sender['host'], packet.sender['port']))
            if self.is_membership_protocol_running is False:
                self.is_membership_protocol_running = True
                self.check_active_nodes(constants.CHECK_INTERVAL)
        print(self.active_nodes)

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