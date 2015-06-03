__author__ = 'Zakaria'

from source import Source
from destination import Destination
from random import randint


#cores or joining nodes.
class Processor(Source, Destination):

    def do(self, message, sender):
        print("processor node: {}", message)
        receiver = self.neighbours[randint(0, self.count_neighbours()-1)]
        self.send(message, receiver[0], receiver[1])

# processor = Processor('127.0.0.1', 12345)
# processor.add_neighbour(('127.0.0.1', '12346'))
# processor.run_server()