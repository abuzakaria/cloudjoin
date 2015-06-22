__author__ = 'Zakaria'


class Packet(object):
    def __init__(self, packet_type):
        self.header = packet_type
        self.data = []

    def set_type(self, packet_type):
        self.header = packet_type

    def append_data(self, data):
        self.data.append(data)
