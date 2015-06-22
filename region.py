__author__ = 'Zakaria'

from collections import deque
from packet import Packet


class Region():
    def __init__(self, store_type, window_size=100):
        self.window_size = window_size
        self.store_type = store_type
        self.queue = deque()      #readonly maxlen is available if needed.

    def store(self, packet):
        if packet.header != self.store_type:
            return False
        if len(self.queue) >= self.window_size:
            self.queue.popleft()
            self.queue.append(packet)
        else:
            self.queue.append(packet)
        return True

    def process(self, guest):
        join_result = Packet('X')
        is_empty = True
        if guest.header == self.store_type:
            return None

        for host in self.queue:
            print(host.header + str(host.data[0]) + ' X ' + guest.header + str(guest.data[0]))

            if host.data[0] == guest.data[0]:   # join matching by id, assumption id is in data[0]
                join_result.data.append(host.data + guest.data)
                is_empty = False
        if is_empty is False:
            join_result.data.append('*')
        else:
            print("---------------------")

        return join_result

    def change_window_size(self, n):
        self.window_size = n
