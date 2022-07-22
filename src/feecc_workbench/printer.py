import asyncio
import os
from pathlib import Path

import httpx
from loguru import logger

from ._image_generation import create_qr, create_seal_tag
from .config import CONFIG
from .database import MongoDbWrapper
from .Messenger import messenger
from .Unit import Unit
from .utils import async_time_execution, get_headers, service_is_up

PRINT_SERVER_ADDRESS: str = CONFIG.printer.print_server_uri


async def print_image(file_path: Path, rfid_card_id: str, annotation: str | None = None) -> None:
    """print the provided image file"""
    if not CONFIG.printer.enable:
        logger.warning("Printer disabled, task dropped")
        return

    if not service_is_up(PRINT_SERVER_ADDRESS):
        messenger.error("Нет связи с сервером печати")
        raise ConnectionError("Printer is not available")

    assert file_path.exists(), f"Image file {file_path} doesn't exist"
    assert file_path.is_file(), f"{file_path} is not an image file"
    task = print_image_task(file_path, rfid_card_id, annotation)

    if CONFIG.printer.skip_ack:
        logger.info(f"Printing task will be executed in the background ({CONFIG.printer.skip_ack=})")
        asyncio.create_task(task)
    else:
        await task


@async_time_execution
async def print_image_task(file_path: Path, rfid_card_id: str, annotation: str | None = None) -> None:
    async with httpx.AsyncClient(timeout=10.0) as client:
        url = f"{PRINT_SERVER_ADDRESS}/print_image"
        headers: dict[str, str] = get_headers(rfid_card_id)
        data = {"annotation": annotation}
        files = {"image_file": open(file_path, "rb")}
        response: httpx.Response = await client.post(url=url, headers=headers, data=data, files=files)

    if response.is_error:
        raise httpx.RequestError(response.json().get("detail", ""))

    logger.info(f"Printed image '{file_path}'")


async def print_unit_barcode(unit: Unit, rfid_card_id: str) -> None:
    if unit.schema.parent_schema_id is None:
        annotation = unit.schema.unit_name
    else:
        parent_schema = await MongoDbWrapper().get_schema_by_id(unit.schema.parent_schema_id)
        annotation = f"{parent_schema.unit_name}. {unit.model_name}."

    try:
        await print_image(Path(unit.barcode.filename), rfid_card_id, annotation=annotation)
    except Exception as e:
        messenger.error(f"Ошибка при печати этикетки: {e}")
        raise e
    finally:
        os.remove(unit.barcode.filename)


async def print_passport_qr_code(unit: Unit, rfid_card_id: str) -> None:
    qrcode_path = create_qr(unit.passport_short_url)

    try:
        if unit.schema.parent_schema_id is None:
            annotation = f"{unit.model_name} (ID: {unit.internal_id}). {unit.passport_short_url}"
        else:
            parent_schema = await MongoDbWrapper().get_schema_by_id(unit.schema.parent_schema_id)
            annotation = (
                f"{parent_schema.unit_name}. {unit.model_name} (ID: {unit.internal_id}). {unit.passport_short_url}"
            )

        await print_image(
            qrcode_path,
            rfid_card_id,
            annotation=annotation,
        )
    except Exception as e:
        messenger.error(f"Ошибка при печати QR-кода: {e}")
    finally:
        os.remove(qrcode_path)


async def print_seal_tag(rfid_card_id: str) -> None:
    seal_tag_img: Path = create_seal_tag()

    try:
        await print_image(seal_tag_img, rfid_card_id)
    except Exception as e:
        messenger.error(f"Ошибка при печати пломбы: {e}")
    finally:
        os.remove(seal_tag_img)
