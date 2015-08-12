__author__ = 'Zakaria'

import time
import constants


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

    def handle_heartbeat(self, packet):
        self.add_node_to_list((packet.sender['host'], packet.sender['port']))
        if self.is_membership_protocol_running is False:
            self.is_membership_protocol_running = True
            self.check_active_nodes(constants.CHECK_INTERVAL)

    def get_nodes(self, n):
        nodelist = []

        if n > len(self.active_nodes):
            return None

        for row in self.active_nodes:
            if n > 0:
                nodelist.append(row[0])
                n -= 1
            else:
                break

        return nodelist
