import asyncio
import logging
from chaski.streamer import ChaskiStreamer

logging.basicConfig(level=logging.DEBUG)


# ----------------------------------------------------------------------
async def run(ip, port, name):
    """"""
    root = ChaskiStreamer(
        ip=ip,
        port=port,
        name=name,
        # root=True,
        paired=True,
        run=False,
    )
    print(f"Root Address: {root.address}")
    asyncio.create_task(root.run())
    while True:
        await root.connect('*ChaskiStreamer@127.0.0.1:65433')
    root


# ----------------------------------------------------------------------
def main(ip='127.0.0.1', port=0, name='ChaskiRoot'):
    """"""
    asyncio.run(run(ip, port, name))


if __name__ == '__main__':
    main()
