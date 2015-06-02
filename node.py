__author__ = 'Zakaria'


#superclass for all types of nodes
class Node:
    host = None
    port = None
    window_size = 100

    def __init__(self, host=None, port=None):
        if host:
            self.host = host
        if port:
            self.port = port