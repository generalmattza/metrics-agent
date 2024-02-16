import asyncio
from typing import Union
from collections import deque
from prometheus_client import Counter

import logging

logger = logging.getLogger(__name__)


def convert_bytes_to_human_readable(num: float) -> str:
    """Convert bytes to a human-readable format."""
    for unit in ["B", "KB", "MB", "GB", "TB", "PB"]:
        if num < 1024.0:
            return f"{num:.2f} {unit}"
        num /= 1024.0
    return f"{num:.2f} {unit}"


class EchoClientProtocol:
    def __init__(self, message, on_con_lost, on_data_received):
        self.message = message
        self.on_con_lost = on_con_lost
        self.on_data_received = on_data_received
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        logger.debug(f"Send: {self.message[:75]}...")
        self.transport.sendto(self.message.encode())

    def datagram_received(self, data, addr):

        logger.debug(f"Received: {data.decode()[:75]}...")
        self.on_data_received(data.decode())

        logger.debug("Close the socket")
        self.transport.close()

    def error_received(self, exc):
        logger.debug(f"Error received: {exc}")

    def connection_lost(self, exc):
        logger.debug("Connection closed")
        if not self.on_con_lost.done():  # Check if the future is already done
            self.on_con_lost.set_result(True)


class AsyncClient:

    def __init__(
        self, buffer: Union[list, deque], server_address: tuple = None, timeout=None
    ):
        self._buffer = buffer
        self.server_address = server_address or ("localhost", 0)
        self.bytes_recvd = Counter("bytes_recvd", "Amount of bytes received from node")
        self.timeout = timeout

    async def request(self, message="", server_address=None, timeout=None):
        server_address = server_address or self.server_address
        timeout = timeout or self.timeout
        loop = asyncio.get_running_loop()

        on_con_lost = loop.create_future()

        def data_received_callback(data):
            self._buffer.append(data)
            bytes_recvd = len(data)
            bytes_recvd_str = convert_bytes_to_human_readable(bytes_recvd)
            self.bytes_recvd.inc(bytes_recvd)
            logger.info(
                f"Received {bytes_recvd_str} from {server_address[0]}:{server_address[1]}"
            )

        transport, protocol = await loop.create_datagram_endpoint(
            lambda: EchoClientProtocol(message, on_con_lost, data_received_callback),
            remote_addr=server_address,
        )

        try:
            if timeout:
                await asyncio.wait_for(on_con_lost, timeout)
            else:
                await on_con_lost
        except asyncio.TimeoutError:
            logging.warning(
                f"Connection to {server_address} timed out after {timeout}s"
            )
        finally:
            transport.close()
