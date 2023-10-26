import asyncio
import random
from asyncio import Queue
from typing import Awaitable, Callable
from enum import StrEnum


class EventType(StrEnum):
    LOGIN = "Login"
    LOGOUT = "Logout"
    PURCHASE = "Purchase"
    NEW = "New" # New event type added



EventQueue = Queue[tuple[EventType, str]]

EventConsumerFn = Callable[[EventType, str], Awaitable[None]]

registered_consumers: dict[EventType, EventConsumerFn] = {}


# Function to register consumers dynamically
def register_consumer(event_type: EventType, consumer: EventConsumerFn):
    registered_consumers[event_type] = consumer


async def general_event_consumer(queue: EventQueue):
    while True:
        event_type, event_data = await queue.get()
        consumer = registered_consumers.get(event_type)
        if consumer:
            await consumer(event_type, event_data)


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
    await asyncio.sleep(10)

# Event generator
async def produce_event(queue: EventQueue):
    while True:
        event_type: EventType = random.choice(
            [event for event in EventType ] # New event type added automatically
        )
        event_data: str = f"Event Data for {event_type}"
        print(f"Produced: {event_type}")
        await queue.put((event_type, event_data))
        await asyncio.sleep(random.uniform(0.5, 1.5))


# Main function to run the event loop
async def main():
    queue: EventQueue = Queue()

    # Register consumers
    register_consumer(EventType.LOGIN, consume_login_event)
    register_consumer(EventType.LOGOUT, consume_logout_event)
    register_consumer(EventType.PURCHASE, consume_purchase_event)
    register_consumer(EventType.NEW, consume_new_event) # New consumer registered

    await asyncio.gather(produce_event(queue), general_event_consumer(queue))


if __name__ == "__main__":
    asyncio.run(main())
