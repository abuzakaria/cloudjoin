__author__ = 'Zakaria'

import time
import constants

#active node row structure. column indexes below
COL_NODE = 0
COL_TIME = 1
COL_USED = 2
COL_SUBW = 3
COL_COST = 4


class MembershipManager:
    active_nodes = []
    is_membership_protocol_running = False
    loop = None

    def __init__(self, loop):
        """

        :param loop:
        """
        self.loop = loop

    def add_node_to_list(self, reporting_node, subwindow_size, cost_value):
        """
        adds node to list if it is a new node. else time is updated of
        matching node
        :param reporting_node:
        """
        for row in self.active_nodes:
            if row:
                if reporting_node == row[COL_NODE]:
                    row[COL_TIME] = time.time()
                    return

        self.active_nodes.append([reporting_node, time.time(), False, subwindow_size, cost_value])
        self.active_nodes.sort(key=lambda x: x[COL_COST], reverse=True)     # sorting active nodes by descending cost
        print("After adding, available nodes:\n\t" + ("\n\t".join(map(str, self.active_nodes))))

    def check_active_nodes(self, interval):
        """
        Checks inactive node after interval
        :param interval:
        """
        current_time = time.time()
        for row in self.active_nodes:
            if row[COL_TIME] + interval < current_time:     # inactive node remove
                self.active_nodes.remove(row)
                print("After removing inactive node, Active nodes:\n\t" + ("\n\t".join(map(str, self.active_nodes))))
        self.loop.call_later(interval, self.check_active_nodes, interval)

    def handle_heartbeat(self, packet):
        """

        :param packet:
        """
        self.add_node_to_list((packet.sender['host'], packet.sender['port']), packet.data[0], packet.data[1])
        if self.is_membership_protocol_running is False:
            self.is_membership_protocol_running = True
            self.check_active_nodes(constants.CHECK_INTERVAL)

    def get_nodes(self, n):
        """

        :param n:
        :return:
        """
        nodelist = []

        for row in self.active_nodes:
            if n > 0 and row[COL_USED] is False:
                row[COL_USED] = True
                nodelist.append([row[COL_NODE], row[COL_SUBW]])
                n -= 1
            elif n <= 0:
                return nodelist

        return None