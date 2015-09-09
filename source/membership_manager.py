__author__ = 'Zakaria'

import time
from core import constants

#active node row structure. column indexes below
COL_NODE = 0
COL_TIME = 1
COL_USED = 2


class MembershipManager:
    active_nodes = []
    is_membership_protocol_running = False
    loop = None

    def __init__(self, loop):
        self.loop = loop

    def add_node_to_list(self, reporting_node):
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

        self.active_nodes.append([reporting_node, time.time(), False])

    def remove_node_from_list(self, inactive_node):
        """
        node removed from active node list
        :param inactive_node:
        """
        for row in self.active_nodes:
            if inactive_node == row[COL_NODE]:
                self.active_nodes.remove(row)

    def check_active_nodes(self, interval):
        """
        Checks inactive node after interval
        :param interval:
        """
        current_time = time.time()
        for row in self.active_nodes:
            if row[COL_TIME] + interval < current_time:
                self.remove_node_from_list(row[COL_NODE])
        self.loop.call_later(interval, self.check_active_nodes, interval)

    def handle_heartbeat(self, packet):
        self.add_node_to_list((packet.sender['host'], packet.sender['port']))
        if self.is_membership_protocol_running is False:
            self.is_membership_protocol_running = True
            self.check_active_nodes(constants.CHECK_INTERVAL)

    def get_nodes(self, n):
        nodelist = []

        for row in self.active_nodes:
            if n > 0 and row[COL_USED] is False:
                row[COL_USED] = True
                nodelist.append(row[COL_NODE])
                n -= 1
            elif n <= 0:
                return nodelist

        return None