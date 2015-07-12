__author__ = 'Zakaria'


#superclass for all types of nodes
class Node:
    name = None
    host = None
    port = None

    def __init__(self, name=None, host=None, port=None):
        if name:
            self.name = name
        if host:
            self.host = host
        if port:
            self.port = port