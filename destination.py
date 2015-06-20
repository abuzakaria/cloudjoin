__author__ = 'Zakaria'
import node
import asyncio


#destination or sink or last node of the network
class Destination(node.Node):

    def run_server(self):
        loop = asyncio.get_event_loop()
        job = asyncio.start_server(self.handle_echo, self.host, self.port, loop=loop)
        server = loop.run_until_complete(job)

        print('MSG: Serving on {}'.format(server.sockets[0].getsockname()))
        try:
            loop.run_forever()
        except KeyboardInterrupt:
            pass

        # Close the server
        server.close()
        loop.run_until_complete(server.wait_closed())
        loop.close()

    @asyncio.coroutine
    def handle_echo(self, reader, writer):
        data = yield from reader.read(self.window_size)
        message = data.decode()
        sender = writer.get_extra_info('peername')
        if message:
            self.do(message, sender)

        # print("Send: %r" % message)
        # writer.write(data)
        # yield from writer.drain()

        print("MSG: Close receiving socket")
        writer.close()

    def do(self, message, sender):
        print(str(sender) + ' >| ' + message + ' |> ' + str(self.port))


#Test run
# from destination import Destination
if __name__ == '__main__':
    dest = Destination('127.0.0.1', 12346)
    dest.run_server()