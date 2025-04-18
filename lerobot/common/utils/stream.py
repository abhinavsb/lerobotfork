import zmq
import numpy as np
from threading import Thread
from queue import Queue

class PubServer:
    def __init__(self, host="*", port=5556):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.bind(f"tcp://{host}:{port}")
        self.queue = Queue()
        self.thread = Thread(target=self._publisher_loop, daemon=True)
        
    def _serialize_array(self, arr):
        """Convert numpy array to C-style bytes with header"""
        header = np.array(arr.shape, dtype=np.uint32).tobytes()
        data = arr.astype(np.float32).tobytes(order='C')
        return header + data

    def _publisher_loop(self):
        while True:
            if not self.queue.empty():
                arr = self.queue.get()
                self.socket.send(self._serialize_array(arr))

    def publish(self, array):
        """Add numpy array to send queue"""
        self.queue.put(array)

    def start(self):
        self.thread.start()


class SubClient:
    def __init__(self, server_ip):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.connect(f"tcp://{server_ip}:5556")
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')

    def receive(self):
        """Receive and reconstruct numpy array"""
        data = self.socket.recv()
        header = np.frombuffer(data[:8], dtype=np.uint32)  # 4 bytes per dim (2D array)
        arr = np.frombuffer(data[8:], dtype=np.float32).reshape(header)
        return arr
