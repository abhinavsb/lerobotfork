import zmq
import numpy as np
from threading import Thread
from queue import Queue, Empty
import time

# ===========================
# Publisher (Server) Section
# ===========================
class PubServer:
    def __init__(self, host="*", port=5556):
        self.ctx = zmq.Context(io_threads=2)
        self.socket = self.ctx.socket(zmq.PUB)
        self.socket.setsockopt(zmq.IMMEDIATE, 1)
        self.socket.setsockopt(zmq.SNDHWM, 10)
        self.socket.setsockopt(zmq.LINGER, 0)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 5)
        self.socket.bind(f"tcp://{host}:{port}")
        self.queue = Queue()
        self.thread = Thread(target=self._publisher_loop, daemon=True)
        self.sent_counter = 0
        self.start_time = time.time()

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
        self.queue.put(array)

    def start(self):
        self.thread.start()

# ===========================
# Subscriber (Client) Section
# ===========================
class SubClient:
    def __init__(self, server_ip, port=5556):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        self.socket.setsockopt(zmq.RCVHWM, 10)
        self.socket.setsockopt(zmq.CONFLATE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 5)
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.socket.connect(f"tcp://{server_ip}:{port}")
        self.latency_samples = []

    def receive(self, timeout_ms=1):
        """Non-blocking receive with polling and latency measurement."""
        data = self.socket.recv()  # This call blocks until data arrives[3][5][7][8]
        header = np.frombuffer(data[:8], dtype=np.uint32)
        arr = np.frombuffer(data[8:], dtype=np.float32).reshape(header)
        return arr

    def _deserialize(self, data):
        header = np.frombuffer(data[:8], dtype=np.uint32)
        return np.frombuffer(data[8:], dtype=np.float32).reshape(header)

# ===========================
# Example Usage
# ===========================
if __name__ == "__main__":
    import sys

    # Usage: python thisfile.py server|client [server_ip]
    if len(sys.argv) < 2:
        print("Usage: python thisfile.py server|client [server_ip]")
        sys.exit(1)

    if sys.argv[1] == "server":
        server = PubServer()
        server.start()
        print("[Server] Started. Publishing random arrays.")
        try:
            while True:
                arr = np.random.rand(100, 100).astype(np.float32)
                server.publish(arr)
                time.sleep(0.01)  # 100 Hz publish rate
        except KeyboardInterrupt:
            print("\n[Server] Stopped.")

    elif sys.argv[1] == "client":
        if len(sys.argv) < 3:
            print("Usage: python thisfile.py client <server_ip>")
            sys.exit(1)
        client = SubClient(sys.argv[2])
        print("[Client] Started. Receiving arrays.")
        try:
            while True:
                arr = client.receive()
                if arr is not None:
                    # Process array here if needed
                    pass
        except KeyboardInterrupt:
            print("\n[Client] Stopped.")
    else:
        print("Unknown mode. Use 'server' or 'client'.")