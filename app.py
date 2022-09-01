# phone/tv/desktop --> server : played(time)
import random
from collections import deque
from dataclasses import dataclass
from enum import Enum
from typing import NewType, Callable, Optional

GLOBAL_TIME_S = 0

TimeFromMovieStart = NewType('TimeFromMovieStart', int)
TimeOffset = NewType('TimeOffset', int)

UserId = NewType('UserId', str)
DeviceId = NewType('DeviceId', str)
MovieId = NewType('MovieId', str)

# this event gets posted when a movie is started or resumed
# this event gets posted every minute when watching a show
EXAMPLE_PLAYED_EVENT = {
    "time_from_movie_start_s": 1231,
    "movie_id": "independence-day",
    "user_id": "manuel@bl0rg.net",
    "device_id": "manuels-phone",
}


@dataclass
class PlayedEvent:
    time_from_move_start_s: TimeFromMovieStart
    movie_id: MovieId
    user_id: UserId
    device_id: DeviceId


class MessageQueue:
    def __init__(self):
        # message is a dequeue
        self.messages = deque()
        self.push_cnt = 0

    def push(self, message):
        self.push_cnt += 1
        if self.push_cnt % 10 == 0:
            event_print(f"{len(self.messages)} messages pushed")
        self.messages.append(message)

    def pop(self):
        return self.messages.popleft()

    def get_message_batch(self, n: int):
        """
        returns a batch of messages if available

        Similar to AWS SQS. Once a message has been read, we mark it as delivered.
        """


class APIServer:
    def __init__(self, name: str, message_queue: MessageQueue):
        self.name = name
        self.message_queue = message_queue

    def post_play_event(self, event):
        event_print(f"{self.name}: POST {event}")

        # probability 50%
        # if random.random() < 0.1:
        #     event_print(f"{self.name}: ERROR DELIVERING PLAY EVENT")
        #     return

        event_print(f"{self.name}: DELIVERING PLAY EVENT {event}")
        self.message_queue.push(event)


class LoadBalancer:
    def __init__(self, servers: [APIServer]):
        self.servers = servers
        self.idx = 0

    # round_robin
    def post_play_event_rr(self, event: PlayedEvent):
        self.servers[self.idx].post_play_event(event)
        self.idx = (self.idx + 1) % len(self.servers)

    def post_play_event_by_device_id(self, event: PlayedEvent):
        # cheap hash function
        server_idx = ord(event.device_id[0]) % len(self.servers)
        self.servers[server_idx].post_play_event(event)

    def post_play_event(self, event: PlayedEvent):
        self.post_play_event_by_device_id(event)


# GLOBAL_API_SERVER = LoadBalancer([
#     APIServer(f"api-server-{idx}") for idx in range(10)])

GLOBAL_MESSAGE_QUEUE = MessageQueue()
GLOBAL_API_SERVER = APIServer("api-server", GLOBAL_MESSAGE_QUEUE)


class DeviceType(Enum):
    IPHONE = 1
    DESKTOP = 2
    TV = 3


def event_print(s):
    print(f"{GLOBAL_TIME_S:5d}: {s}")


class VideoPlayer:
    def __init__(self, device_type: DeviceType, device_id: DeviceId, user_id: UserId, movie_id: MovieId):
        self.device_type = device_type
        self.device_id = device_id
        self.user_id = user_id
        self.current_time_from_start_s = TimeFromMovieStart(0)
        self.time_since_last_play_event_s = TimeFromMovieStart(0)
        self.movie_id = movie_id
        self.time_s = 0
        self.playing = False

    def __str__(self):
        return f"{self.device_type}({self.user_id}@{self.device_id}: {self.movie_id}@{self.current_time_from_start_s})"

    def start(self):
        event_print(f"{self}.START")
        self.playing = True
        self.post_play_event()

    def pause(self):
        event_print(f"{self}.PAUSE")
        self.playing = False
        self.post_play_event()

    def stop(self):
        event_print(f"{self}.STOP")
        self.playing = False
        self.post_play_event()

    def skip(self, time_offset_s: TimeOffset):
        self.current_time_from_start_s += time_offset_s
        event_print(f"{self}.SKIP {time_offset_s}")

    def seek(self, time_from_start_s: TimeFromMovieStart):
        self.current_time_from_start_s = time_from_start_s
        event_print(f"{self}.SEEK {time_from_start_s}")

    def post_play_event(self):
        self.time_since_last_play_event_s = 0
        event_print(f"{self}.POST_PLAY_EVENT")

        global GLOBAL_API_SERVER
        GLOBAL_API_SERVER.post_play_event(
            PlayedEvent(self.current_time_from_start_s,
                        self.movie_id,
                        self.user_id,
                        self.device_id))

    def tick(self):
        """
        This is called every second
        :return:
        """
        self.time_s += 1
        self.time_since_last_play_event_s += 1
        if self.playing:
            self.current_time_from_start_s += 1
            if self.time_since_last_play_event_s >= 60:
                self.post_play_event()


@dataclass
class Event:
    time_s: int
    description: Optional[str]
    function: Callable[[], None]


def main():
    manuelsVideoPlayer = VideoPlayer(device_type=DeviceType.IPHONE,
                                     device_id=DeviceId('manuels-iphone'),
                                     user_id=UserId('manuel'),
                                     movie_id=MovieId('independence-day')
                                     )
    thorsVideoPlayer = VideoPlayer(device_type=DeviceType.IPHONE,
                                   device_id=DeviceId('thors-iphone'),
                                   user_id=UserId('thor'),
                                   movie_id=MovieId('beethoven')
                                   )
    nanasVideoPlayer = VideoPlayer(device_type=DeviceType.IPHONE,
                                   device_id=DeviceId('nanas-iphone'),
                                   user_id=UserId('nana'),
                                   movie_id=MovieId('lassie')
                                   )

    players = [
        manuelsVideoPlayer,
        thorsVideoPlayer,
        nanasVideoPlayer
    ]

    events = [
        Event(
            time_s=0,
            description="manuel starts playing independence-day",
            function=lambda: manuelsVideoPlayer.start())
        , Event(
            time_s=10,
            description="thor starts playing beethoven",
            function=lambda: thorsVideoPlayer.start())
        , Event(
            time_s=20,
            description="nana starts playing lassie",
            function=lambda: nanasVideoPlayer.start())
    ]

    global GLOBAL_TIME_S

    # the universe starts
    for time_s in range(60 * 60):
        GLOBAL_TIME_S = time_s
        if time_s % 30 == 0:
            event_print("TICK")

        for event in events:
            if event.time_s == time_s:
                event.function()

        for player in players:
            player.tick()


if __name__ == "__main__":
    main()
