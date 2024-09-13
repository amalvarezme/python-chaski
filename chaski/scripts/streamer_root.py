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
        root=True,
        paired=True,
        run=False,
    )
    print(f"Root Address: {root.address}")
    await root.run()


# ----------------------------------------------------------------------
def main(ip=None, port=65433, name='ChaskiRoot'):
    """"""
    asyncio.run(run(ip, port, name))


if __name__ == '__main__':
    main()
