__author__ = 'Zakaria'


class Region():
    def __init__(self, window_size=100):
        self.window_size = window_size

    def change_window_size(self, n):
        self.window_size = n
