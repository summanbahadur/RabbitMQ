import pika
import time
from datetime import datetime


# Global variable to keep track of the number of cameras
camera_count = None
last_message_time = {}
event_log_first =0 #we can skip detection when event is loged for first time
def get_camera_count():
    global camera_count
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='camera_count')
    method_frame, header_frame, body = channel.basic_get(queue='camera_count', auto_ack=True)
    if body:
        camera_count = int(body)
    else:
        camera_count = 0

    connection.close()

def handle_message(channel, method, properties, body):
    global last_message_time
    global event_log_first

    if event_log_first ==0: 
        get_camera_count()
        create_dictionary()
        event_log_first = event_log_first+1

    # Convert the message body to a dictionary
    camera_id, message_time = body.decode().split(',')
    last_message_time[camera_id] = message_time
    print(f"Received camera {camera_id} status message: %s" % body)

    channel.basic_ack(delivery_tag=method.delivery_tag)
    detect_offline_cameras()


def detect_offline_cameras():
    global last_message_time
    global camera_count
    current_count = len(last_message_time)
    
    for camera_id in range(camera_count):
        last_time_str = last_message_time.get(str(camera_id),"invalid")
        if last_time_str != "invalid":
            last_time = datetime.strptime(last_time_str, "%H:%M:%S")
            # Check if the message is more than 2 minutes old
            current_time_str = time.strftime("%H:%M:%S", time.localtime())
            current_time = datetime.strptime(current_time_str, "%H:%M:%S")
            delta= current_time - last_time
            mins = divmod(delta.total_seconds(), 60)[0]  
            #print(f" {mins} , {camera_id}")
            if (mins >= 2):
                print(f"Camera {camera_id} is offline for more than 2 minutes.")


def create_dictionary():
    global last_message_time
    global camera_count
    for camera_id in range(camera_count):
        current_time_str = time.strftime("%H:%M:%S", time.localtime())
        last_message_time[camera_id] = current_time_str


def start_consuming():
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.queue_declare(queue='camera_events')
   # get_camera_count()
    #create a dictionary with initial timestamp
    #create_dictionary()
 
    channel.basic_consume(queue='camera_events', on_message_callback=handle_message)

    #Loop for checking offline cameras 
    print("waiting for messages..")
    channel.start_consuming()



if __name__ == '__main__':
    start_consuming()
