import asyncio
import typing as tp

import httpx
from loguru import logger

from .config import CONFIG
from .utils import async_time_execution, get_headers

PRINT_SERVER_ADDRESS: str = CONFIG.printer.print_server_uri


async def print_image(file_path: str, rfid_card_id: str, annotation: tp.Optional[str] = None) -> None:
    """print the provided image file"""
    if not CONFIG.printer.enable:
        logger.warning("Printer disabled, task dropped")
        return

    task = print_image_task(file_path, rfid_card_id, annotation)

    if CONFIG.printer.skip_ack:
        logger.info(f"Printing task will be executed in the background ({CONFIG.printer.skip_ack=})")
        asyncio.create_task(task)
    else:
        await task


@async_time_execution
async def print_image_task(file_path: str, rfid_card_id: str, annotation: tp.Optional[str] = None) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = f"{PRINT_SERVER_ADDRESS}/print_image"
        headers: tp.Dict[str, str] = get_headers(rfid_card_id)
        data = {"annotation": annotation}
        files = {"image_file": open(file_path, "rb")}
        response: httpx.Response = await client.post(url=url, headers=headers, data=data, files=files)

    if response.is_error:
        raise httpx.RequestError(response.text)

    logger.info(f"Printed image '{file_path}'")
