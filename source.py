__author__ = 'Zakaria'
import node
import asyncio
import time
import constants
from packet import Packet


#source or first node of the network
class Source(node.Node):
    active_nodes = []
    is_membership_protocol_running = False
    temp_flag_sending = False
    round_robin_counter = 0        #used for round robin: in which node to store

    def add_node_to_list(self, reporting_node):
        """
        adds node to list if it is a new node. else time is updated of
        matching node
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
        node removed from active node list
        :param inactive_node:
        """
        for row in self.active_nodes:
            if inactive_node == row[0]:
                self.active_nodes.remove(row)

    def check_active_nodes(self, interval):
        """
        Checks inactive node after interval
        :param interval:
        """
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
        node_count = len(self.active_nodes)
        i = 0
        print(node_count)
        for row in self.active_nodes:
            if row:
                (host, port) = row[0]
                if i == self.round_robin_counter % node_count:
                    packet.store = True
                    self.round_robin_counter += 1
                else:
                    packet.store = False
                self.send(packet, host, port)
            i += 1

    def change_window_size(self, n, size):
        """

        :param n: node to be changed
        :param size: new window size
        """
        p = Packet(constants.DATATYPE_CONFIG)
        p.append_data(size)
        self.send(p, n[0], n[1])

    def do(self, packet):
        """

        :param packet:
        """
        print(packet.type + ' from ' + packet.sender['name'])
        if packet.type == constants.DATATYPE_HEARTBEAT:
            self.add_node_to_list((packet.sender['host'], packet.sender['port']))
            if self.is_membership_protocol_running is False:
                self.is_membership_protocol_running = True
                self.check_active_nodes(constants.CHECK_INTERVAL)
            if self.temp_flag_sending is False and len(self.active_nodes) >= constants.MINIMUM_NODES:
                self.start_sending_stream()
                self.temp_flag_sending = True
        print(self.active_nodes)

    def start_sending_stream(self):
        with open("data.txt") as f:
            lines = f.read().splitlines()
            l = len(lines)
            for i in range(0, l, 3):    # loop through 3 lines at a time
                pack = Packet(lines[i])
                pack.append_data(lines[i+1])
                pack.append_data(lines[i+2])

                self.distribute(pack)
                # self.run_server()
                # src.send(pack, '127.0.0.1', 12350)
                # time.sleep(1)


###########################################################






#
# #Test run
if __name__ == '__main__':
    src = Source('source', '127.0.0.1', '12344')
    src.run_server()
#     hst = '127.0.0.1'
#     # add processor nodes as neighbors
#     # for prt in range(12345, 12350):
#     #     src.add_neighbour((hst, prt))
#
#     get_data()
#     l = len(lines)
#     for i in range(0, l, 3):    # loop through 3 lines at a time
#         pack = Packet(lines[i])
#         pack.append_data(lines[i+1])
#         pack.append_data(lines[i+2])
#
#         # src.distribute(pack)
#         src.run_server()
#         # src.send(pack, '127.0.0.1', 12350)
#         time.sleep(1)