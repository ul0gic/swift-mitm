from http.server import ThreadingHTTPServer
from socketserver import TCPServer
import sys


class LoopbackHTTPServer(ThreadingHTTPServer):
    def server_bind(self):
        TCPServer.server_bind(self)
        print("origin-start: socket-bound", file=sys.stderr, flush=True)
        # HTTPServer.server_bind performs reverse DNS; these fixtures already know their loopback identity.
        self.server_name = "localhost"
        self.server_port = self.server_address[1]
