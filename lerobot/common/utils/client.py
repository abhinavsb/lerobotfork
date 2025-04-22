import zmq
import numpy as np
import rerun as rr
import time
import argparse

# ===========================
# Optimized Subscriber Client
# ===========================
class VisionClient:
    def __init__(self, server_ip, port=5556):
        self.ctx = zmq.Context()
        self.socket = self.ctx.socket(zmq.SUB)
        
        # ZeroMQ optimization settings
        self.socket.setsockopt(zmq.RCVHWM, 5)
        self.socket.setsockopt(zmq.CONFLATE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE, 1)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_IDLE, 30)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_INTVL, 5)
        self.socket.setsockopt(zmq.TCP_KEEPALIVE_CNT, 3)
        self.socket.setsockopt(zmq.AFFINITY, 1)  # Dedicate CPU core
        
        self.socket.connect(f"tcp://{server_ip}:{port}")
        self.socket.setsockopt(zmq.SUBSCRIBE, b'')
        self.poller = zmq.Poller()
        self.poller.register(self.socket, zmq.POLLIN)
        
        self.latest_images = {}

    def receive(self):
        """Non-blocking receive with latest image per camera"""
        while True:
            events = dict(self.poller.poll(1))  # 1ms timeout
            if self.socket in events:
                topic, data = self.socket.recv_multipart()
                header = np.frombuffer(data[:12], dtype=np.uint32)
                shape = (header[0], header[1], header[2])
                img = np.frombuffer(data[12:], dtype=np.uint8).reshape(shape)
                self.latest_images[topic.decode()] = img
            else:
                break

# ===========================
# Rerun Visualization Setup
# ===========================
def configure_rerun(camera_names):
    rr.init("Robot Camera Stream", spawn=True)
    rr.memory_recording()  # Lowest latency mode
    rr.set_global_data_recording(True)  # Batch updates
    
    # Create tiled layout
    rr.log_viewport(
        "camera_view",
        rr.Viewport(
            children=[rr.Viewport(f"cameras/{name}") for name in camera_names],
            layout=rr.GridLayout(
                columns=2,  # Adjust based on number of cameras
                col_shares=[1]*2,
                row_shares=[1]*(len(camera_names)//2 + 1)
            )
        )
    )

# ===========================
# Main Execution
# ===========================
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Robot Vision Client')
    parser.add_argument('server_ip', help='Server IP address')
    parser.add_argument('-p', '--port', type=int, default=5556)
    parser.add_argument('--cameras', nargs='+', default=['left', 'right'],
                        help='List of expected camera names')
    args = parser.parse_args()

    # Initialize systems
    configure_rerun(args.cameras)
    client = VisionClient(args.server_ip, args.port)
    
    last_update = time.monotonic()
    try:
        while True:
            # Process all incoming messages
            client.receive()
            
            # Update display at 30Hz (33.33ms interval)
            current_time = time.monotonic()
            if (current_time - last_update) >= 0.0333:
                for cam_name, img in client.latest_images.items():
                    rr.log(f"cameras/{cam_name}", rr.Image(img))
                
                rr.set_time_seconds("capture_time", time.time())
                last_update = current_time
            
            time.sleep(0.001)  # Prevent CPU spin

    except KeyboardInterrupt:
        print("\n[Client] Stopped gracefully.")