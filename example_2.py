import asyncio
import random
from asyncio import Queue,PriorityQueue
from typing import Awaitable, Callable
from enum import StrEnum, Enum
from dataclasses import dataclass, Field


class EventType(StrEnum):
    LOGIN = "Login"
    LOGOUT = "Logout"
    PURCHASE = "Purchase"
    NEW = "New" # New event type added


class Priority(Enum):
    LOW = 3
    MEDIUM = 2
    HIGH = 1
    
    def __lt__(self, other):
        if self.__class__ is other.__class__:
            return self.value < other.value
        return NotImplemented
    
@dataclass(order=True)
class Event:
    priority: Priority
    event_type: EventType
    event_data: str

 



EventQueue = PriorityQueue()

EventConsumerFn = Callable[[EventType, str], Awaitable[None]]

registered_consumers: dict[EventType, EventConsumerFn] = {}



# Function to register consumers dynamically
def register_consumer(event_type: EventType, consumer: EventConsumerFn):
    registered_consumers[event_type] = consumer


async def general_event_consumer(queue: EventQueue):
    while True:
        event:Event = await queue.get()
        consumer = registered_consumers.get(event.event_type)
        if consumer:
            await consumer(event.event_type, event.event_data)
        else:
            print(f"No consumer registered for event type {event.event_type}")


# Specific consumers
async def consume_login_event(_: EventType, event_data: str) -> None:
    print(f"Consuming Login Event: {event_data}")
    await asyncio.sleep(1)


async def consume_logout_event(_: EventType, event_data: str) -> None:
    print(f"Consuming Logout Event: {event_data}")
    await asyncio.sleep(1)


async def consume_purchase_event(_: EventType, event_data: str) -> None:
    print(f"Consuming Purchase Event: {event_data}")
    await asyncio.sleep(1)

# New consumer
async def consume_new_event(_: EventType, event_data: str) -> None:
    print(f"Consuming New Event: {event_data}")
    await asyncio.sleep(2)

# Event generator
async def produce_event(queue: EventQueue):
    while True:
        event_type: EventType = random.choice(
            [event for event in EventType ] # New event type added automatically
        )
        event_priority: Priority = random.choice(
            [priority for priority in Priority]
        )
        event_data: str = f"Event Data for {event_type} priority: {event_priority}"
        print(f"Produced: {event_type} priority: {event_priority}")
        
         
        await queue.put(Event(event_priority,event_type, event_data))
        #print(f"Queue: {queue}")
        await asyncio.sleep(random.uniform(0.5, 1.5))


# Main function to run the event loop
async def main():
    queue: EventQueue = PriorityQueue()

    # Register consumers
    register_consumer(EventType.LOGIN, consume_login_event)
    register_consumer(EventType.LOGOUT, consume_logout_event)
    register_consumer(EventType.PURCHASE, consume_purchase_event)
    register_consumer(EventType.NEW, consume_new_event) # New consumer registered

    await asyncio.gather(produce_event(queue), general_event_consumer(queue))


if __name__ == "__main__":
    asyncio.run(main())
