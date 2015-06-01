__author__ = 'Zakaria'

import source
import destination


class Processor(source.Source, destination.Destination):

    def test(self):
        print("processor node")



