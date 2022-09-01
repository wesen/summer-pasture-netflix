from collections import deque
import random

GLOBAL_TIME_S = 0


def event_print(s):
    print(f"{GLOBAL_TIME_S:5d}: {s}")


class MessageQueue:
    def __init__(self):
        # message is a dequeue
        self.messages = deque()

    def push(self, message):
        self.messages.append(message)

    def get_message_batch(self, n: int):
        ret = []
        for i in range(n):
            if len(self.messages) == 0:
                break
            ret.append(self.messages.popleft())

        return ret

    def size(self):
        return len(self.messages)


class MessageQueueWithRetries:
    def __init__(self):
        # message is a dequeue
        self.messages = deque()
        self.grabbed = {}
        self.graveyard = []
        self.cnt = 0
        self.monitoring_alerts = None

    def push(self, message):
        self.cnt += 1
        self.messages.append({"id": self.cnt,
                              "delivered_cnt": 0,
                              "message": message})

    def get_message_batch(self, n: int):
        ret = []
        for i in range(n):
            if len(self.messages) == 0:
                break
            msg = self.messages.popleft()
            msg["delivered_cnt"] += 1
            self.grabbed[msg["id"]] = msg
            ret.append(msg)

        return ret

    def mark_message_as_delivered(self, id_):
        # XXX
        self.grabbed[id_] = None

    def mark_message_as_failed(self, id_):
        if id_ in self.grabbed:
            msg = self.grabbed[id_]
            if msg["delivered_cnt"] > 3:
                event_print(f"{msg['id']} sent to graveyard")
                self.graveyard.append(msg)
            event_print(f"{id_} failed, reinsert into queue, {msg['delivered_cnt']} deliveries")
            self.messages.append(msg)
            self.grabbed[id_] = None

    def size(self):
        return len(self.messages)


class Producer:
    def __init__(self, name: str, message_queue: MessageQueue):
        self.name = name
        self.message_queue = message_queue

    def deliver_message(self, message):
        self.message_queue.push(message)


class Consumer:
    def __init__(self, name: str, message_queue: MessageQueue):
        self.name = name
        self.message_queue = message_queue

    def tick(self):
        for message in self.message_queue.get_message_batch(10):
            print(f"{self.name}: {message}")


class ConsumerWithRetries:
    def __init__(self, name: str, message_queue: MessageQueueWithRetries):
        self.name = name
        self.message_queue = message_queue

    def tick(self):
        for message in self.message_queue.get_message_batch(10):
            # probability 50% to fail delivery
            if random.random() < 0.7:
                self.message_queue.mark_message_as_failed(message["id"])
            else:
                event_print(f"{self.name}: handled {message}")
                self.message_queue.mark_message_as_delivered(message["id"])


def main_2_consumers_single_delivery():
    message_queue = MessageQueue()
    producer = Producer("producer", message_queue)
    consumer = Consumer("consumer", message_queue)
    consumer2 = Consumer("consumer2", message_queue)

    global GLOBAL_TIME_S

    msg_id = 0
    for i in range(100):
        producer.deliver_message(f"message {msg_id}")
        msg_id += 1

    for i in range(10):
        GLOBAL_TIME_S += 1
        consumer.tick()
        consumer2.tick()

        for j in range(random.randint(0, 10)):
            producer.deliver_message(f"message r{msg_id}")
            msg_id += 1

        event_print(f"{message_queue.size()} messages in queue")


def main_single_consumer_with_retry():
    message_queue = MessageQueueWithRetries()
    producer = Producer("producer", message_queue)
    consumer = ConsumerWithRetries("consumer", message_queue)

    global GLOBAL_TIME_S

    msg_id = 0
    for i in range(100):
        producer.deliver_message({
            "payload": f"message {msg_id}",
            "is_error": random.randint(0, 1) == 0})
        msg_id += 1

    for i in range(100):
        GLOBAL_TIME_S += 1
        consumer.tick()

        event_print(f"{message_queue.size()} messages in queue")

    print(f"GRAVEYARD: {message_queue.graveyard}")


if __name__ == "__main__":
    main_single_consumer_with_retry()
