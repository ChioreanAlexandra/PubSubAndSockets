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
    TIME_LIMIT = 20

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

        self.publisher = pubsub_v1.PublisherClient()
        self.publications = publications

        self.saved_message = ""
        self.port_server = port_server

    def subscription(self):
        print(f"Socket {self.id} is subscribed")
        while True:
            for topic in self.subscriptions:
                print(f"Check {topic}")
                subscription_path = self.subscriber.subscription_path(self.PROJECT_ID, 'sub_' + topic.lower())
                response = self.subscriber.pull(
                    request={"subscription": subscription_path, "max_messages": 1},
                    retry=retry.Retry(deadline=30),
                )
                for message in response.received_messages:
                    self.subscriber.acknowledge(
                        request={"subscription": subscription_path, "ack_ids": [message.ack_id]}
                    )
                    received_message = message.message.data.decode('utf-8')
                    if received_message != self.saved_message:
                        logging.info(f"<<{topic}>> " + received_message)

    def publish(self):
        start_time = time.process_time()
        topic_index = random.randint(0, len(self.publications) - 1)
        topic = self.publications[topic_index]
        self.saved_message = input("Enter idea about " + topic + ": ")
        # message = "Nana"
        print("This is the entered message: " + self.saved_message)
        topic_path = self.publisher.topic_path(self.PROJECT_ID, topic.lower())
        if time.process_time() - start_time < self.TIME_LIMIT and self.saved_message != "":
            self.publisher.publish(topic_path, self.saved_message.encode('utf-8'))

    def start(self):
        self.client_socket.connect((app.HOST, app.port_client))
        sockets_list = [self.client_socket]
        conn, add = self.server_socket.accept()
        with conn:
            while True:
                # server side
                if self.token != "":
                    input_thread = threading.Thread(target=self.publish)
                    input_thread.start()
                    start_time = time.process_time()
                    init_msg = self.saved_message
                    while time.process_time() - start_time < self.TIME_LIMIT and self.saved_message == init_msg:
                        pass
                    if self.saved_message == init_msg:
                        print("Your time to enter a subject is finished, please press enter to finish this attempt")
                    input_thread.join()
                    print("Done")
                    msg = self.token
                    conn.send(msg.encode('utf-8'))
                    self.token = ""
                read_sockets, write_socket, error_socket = select.select(sockets_list, [], [])
                for socks in read_sockets:
                    if socks == self.client_socket:
                        # if we receive the socket it means that we have to set it and start processing
                        message = socks.recv(2048).decode('utf-8')
                        self.token = str(self.id) + str(message[1:])
                        print(f"Token value: {message}, for socket with id: {self.id}")


if __name__ == '__main__':
    argvs = input("Args:")
    argvs = argvs.split(" ")
    thread_id = int(argvs[0])
    server_port = int(argvs[1])
    client_port = int(argvs[2])
    pub_topics = [argvs[3], argvs[4]]
    sub_topics = [argvs[5], argvs[6]]
    app = ApplicationClass(thread_id, server_port, client_port, sub_topics, pub_topics)
    thread_subscription = threading.Thread(target=app.subscription)
    thread_subscription.start()
    time.sleep(10)
    app.start()
