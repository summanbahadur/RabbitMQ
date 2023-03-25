import threading
import random
import time
import pika
import datetime

class CameraEmulator(threading.Thread):
    def __init__(self, camera_id):
        super().__init__()
        self.camera_id = camera_id
        self.connection = pika.BlockingConnection(pika.ConnectionParameters('localhost',heartbeat=0))
        self.channel = self.connection.channel()
        self.channel.queue_declare(queue='camera_events')
        self.can_run = threading.Event()
        self.thing_done = threading.Event()
        self.thing_done.set()
        self.can_run.set() 
        
    def run(self):
        while True:
            self.can_run.wait()
            try:
                self.thing_done.clear()
                self.send_events()
                time.sleep(10)

            finally:
                self.thing_done.set()
        
       

    def send_events(self):
        current_time = time.strftime("%H:%M:%S", time.localtime())
        message = f"{self.camera_id},{current_time}"
        print(f"Sending event for camera {self.camera_id}")
        self.channel.basic_publish(exchange='', routing_key='camera_events', body=message)
           
    def pause(self):
        print(f"Camera {self.camera_id} is going offline...")
        self.can_run.clear()
        self.thing_done.wait()
        offline_duration=300 #for testing
        time.sleep(offline_duration)
        print(f"Camera {self.camera_id} is going online...")
        self.can_run.set() 

    def resume(self):
        self.can_run.set()    

   


connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='camera_count')
num_cameras = int(input("Enter number of cameras to emulate: "))
cameras = [CameraEmulator(i) for i in range(num_cameras)]

# Publish camera count to queue
channel.basic_publish(exchange='', routing_key='camera_count', body=str(num_cameras))

for camera in cameras:
    camera.start()

# Randomly select a number of cameras to go offline
num_cameras_offline = random.randint(0, num_cameras)
#num_cameras_offline=1
cameras_offline = random.sample(cameras, num_cameras_offline)

for camera in cameras_offline:
    camera.pause()


