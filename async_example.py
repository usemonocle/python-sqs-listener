import asyncio
import signal

from sqs_launcher.asyncio import AsyncSqsLauncher
from sqs_listener.asyncio import AsyncSqsListener


class Listener(AsyncSqsListener):

    async def handle_message(self, body, attributes, messages_attributes):
        print(body)


async def run_listener():
    listener = Listener(queue='matan-test',
                        interval=10,
                        max_number_of_messages=5,
                        max_messages_parallelism=100,
                        wait_time=10,
                        region_name='us-east-1')
    task1 = asyncio.create_task(listener.listen())

    def kill(signal, frame):
        listener.stop()

    signal.signal(signal.SIGINT, kill)
    signal.signal(signal.SIGTERM, kill)
    await task1


async def run_producer():
    launcher = AsyncSqsLauncher('matan-test', create_queue=True, region_name='us-east-1')
    tasks = [asyncio.create_task(launcher.launch_message(f'{{"a":"{i}"}}'))
             for i in range(1_000)]

    await asyncio.gather(*tasks)


if __name__ == '__main__':
    asyncio.run(run_producer())
    asyncio.run(run_listener())

