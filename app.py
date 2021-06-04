import logging
import socket
import threading
import time
import random
import os
import select
from google.cloud import pubsub_v1
from google.api_core import retry

os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = r"C:\Users\Asus\Downloads\cpdproject-1622384814623-d1ab850eb52a.json"
logging.basicConfig(level=logging.INFO)


class ApplicationClass:
    HOST = "127.0.0.1"
    PROJECT_ID = "cpdproject-1622384814623"
    TIME_LIMIT = 30

    def __init__(self, id, port_server, port_client, subscriptions, publications):
        self.id = id
        self.token = "" if id != 1 else "1This is the token"
        self.server_socket = socket.socket()
        self.server_socket.bind((self.HOST, port_server))
        self.server_socket.listen(1)

        self.client_socket = socket.socket()
        self.port_client = port_client

        self.subscriber = pubsub_v1.SubscriberClient()
        self.subscriptions = subscriptions
        # self.create_subscriptions(subscriptions)

        self.publisher = pubsub_v1.PublisherClient()
        self.publications = publications

        self.saved_message = ""

    def callback(self, message):
        received_message = message.data.decode('utf-8')
        topic = message.attributes['topic_name']
        if received_message != self.saved_message:
            logging.info(f"<<{self.id}>>: <<{topic}>>: " + received_message)
        message.ack()

    def create_subscriptions(self, subscriptions):
        for topic in subscriptions:
            topic_name = 'projects/{project_id}/topics/{topic}'.format(
                project_id=self.PROJECT_ID,
                topic=topic.lower(),  # Set this to something appropriate.
            )

            subscription_name = 'projects/{project_id}/subscriptions/{sub}'.format(
                project_id=self.PROJECT_ID,
                sub='sub_' + str(self.id) + '_' + topic.lower(),  # Set this to something appropriate.
            )
            self.subscriber.create_subscription(
                name=subscription_name, topic=topic_name)

    def subscription(self):
        print(f"Socket {self.id} is subscribed")
        flow_control = pubsub_v1.types.FlowControl(max_messages=1)

        for topic in self.subscriptions:
            subscription_path = self.subscriber.subscription_path(self.PROJECT_ID,
                                                                  'sub_' + str(self.id) + '_' + topic.lower())
            future = self.subscriber.subscribe(subscription_path, self.callback,  flow_control=flow_control)

    def publish(self):
        start_time = time.process_time()
        topic_index = random.randint(0, len(self.publications) - 1)
        topic = self.publications[topic_index]
        self.saved_message = input("Enter idea about " + topic + ": ")
        topic_path = self.publisher.topic_path(self.PROJECT_ID, topic.lower())
        if time.process_time() - start_time < self.TIME_LIMIT and self.saved_message != "":
            self.publisher.publish(topic_path, self.saved_message.encode('utf-8'), topic_name=topic)

    def start(self):
        self.client_socket.connect((app.HOST, app.port_client))
        sockets_list = [self.client_socket]
        conn, add = self.server_socket.accept()
        with conn:
            while True:
                # server side
                if self.token != "":
                    time.sleep(2)
                    input_thread = threading.Thread(target=self.publish)
                    input_thread.start()
                    start_time = time.process_time()
                    init_msg = self.saved_message
                    while time.process_time() - start_time < self.TIME_LIMIT and self.saved_message == init_msg:
                        pass
                    if self.saved_message == init_msg:
                        print("Your time to enter a subject is finished, please press enter to finish this attempt")
                    input_thread.join()
                    msg = self.token
                    conn.send(msg.encode('utf-8'))
                    print("Token sent")
                    self.token = ""
                read_sockets, write_socket, error_socket = select.select(sockets_list, [], [])
                for socks in read_sockets:
                    if socks == self.client_socket:
                        # if we receive the socket it means that we have to set it and start processing
                        message = socks.recv(2048).decode('utf-8')
                        self.token = str(self.id) + str(message[1:])
                        # print(f"Token value: {message}, for socket with id: {self.id}")


if __name__ == '__main__':
    argvs = input("Args:")
    argvs = argvs.split(" ")
    thread_id = int(argvs[0])
    server_port = int(argvs[1])
    client_port = int(argvs[2])
    pub_topics = [argvs[3], argvs[4]]
    sub_topics = [argvs[5], argvs[6]]
    app = ApplicationClass(thread_id, server_port, client_port, sub_topics, pub_topics)
    app.subscription()
    time.sleep(10)
    app.start()
