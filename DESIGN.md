# Design a notification system

## Problem

We want to deliver notifications
to every device a user uses when the following things happen:

- the user viewed a show
    1. what does "viewing a show mean?"
       1. watching a show for more than 1 minute
       2. every minute afterwards
       3. what about show rollovers?
    2. what does that notification do?
       1. shows how far the show was played on all the other device
    3. this event will be sent around once a minute when a user is watching
    4. not a super important event

- the user has left the TV on and the TV is just auto playing
  - the app asks the user after 5 episodes have been played
  - just handle it normally

- the user watched a show and gets new recommendations
    1. when do recommendations get computed?
       1. every night, a big ML job runs to recompute the recommendations
    2. what does the event do?
       1. display new frontpage on the device
    3. once a day
    4. pretty important

- the user changes their membership plan
  1. when does it happen?
     1. when they change their membership plan
  2. what does the event do?
     1. immediately update the capabilities of the device
  3. very important
  4. what happens when a user that just canceled their membership 
     is able to still stream without having received the notification

### Scale that we are currently at

1000 active users at any one time, 50k total users 
   = 60000 play notifications per hour
   = 17 / sec
50000 / 700 = 70 users change membership per day = 100 = 5 per hour
50k new recommendations per day = 50k at midnight

### Scale that we will reach in 2 years

1M active users at any one time, 50M total users

60M play notifications per hour = 15k / second 
5000 membership notifications per hour = 2 / second
50M new recommendations per day

### Current stack

We have a streaming service and we have:

- ios app
- android app
- webapp

For the non-streaming part, we have a standard flask app with no database.

On the team we have:
- 2 full-stack engineers that know python and some linux

## Brainstorm

- single source of truth
  - middleman
    - the middleman verifies that the membership cancellation
    - the middleman broadcasts that to the other devices

@startuml
:user:

:user: --> (Cancels the service)
:payment processor: --> (Cancels the service)
(Cancels the service) -> :middleman:
:middleman: --> [Database]
:middleman: -> (Notify the devices)
(Notify the devices) --> [ios]
(Notify the devices) --> [android]
(Notify the devices) --> [webapp]

@enduml



@startuml
user -> server : give me JS1
server -> user : JS1
user -> server : give me JS2
server -> user : JS2
user -> server : give me JS3
server -> user : JS3
user -> server : give me JS4
server -> user : JS4
user -> server : give me JS5
server -> user : JS5
@enduml


@startuml
user -> server : give me all JS
server -> user : JS1
server -> user : JS2
server -> user : JS3
server -> user : JS4
server -> user : JS5
@enduml

@startuml
user -> edgeNode : startStream
edgeNode -> mainNode : canUserStream?
mainNode -> edgeNode : yes
edgeNode -> user : stream1
edgeNode -> user : stream2
edgeNode -> user : stream3
edgeNode -> user : stream4
edgeNode -> user : stream5
edgeNode -> user : stream6
edgeNode -> user : stream7
edgeNode -> user : stream8
user -> edgeNode : pause
@enduml

## Fundamentals

- latency
- bandwidth
- batch / single processing
- caches

























