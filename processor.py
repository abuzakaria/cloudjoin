__author__ = 'Zakaria'

import source
import destination


#cores or joining nodes.
class Processor(source.Source, destination.Destination):

    def join(self, mode='split'):
        print("processor node: {}", self.mode)