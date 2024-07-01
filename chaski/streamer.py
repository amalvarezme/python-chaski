from asyncio import Queue
from chaski.node import ChaskiNode, Edge, Message


########################################################################
class ChaskiStreamer(ChaskiNode):
    """"""

    # ----------------------------------------------------------------------
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.message_queue = Queue()

    # ----------------------------------------------------------------------
    async def __aenter__(self):
        """"""
        return self.message_queue

    # ----------------------------------------------------------------------
    async def __aexit__(self, exception_type, exception_value, exception_traceback):
        """"""
        pass

    # ----------------------------------------------------------------------
    async def _process_ChaskiMessage(self, message: Message, edge: Edge):
        """"""
        await self.message_queue.put(message)

    # ----------------------------------------------------------------------
    async def write(self, topic, data):
        """"""
        await self._write('ChaskiMessage', data=data, topic=topic)
