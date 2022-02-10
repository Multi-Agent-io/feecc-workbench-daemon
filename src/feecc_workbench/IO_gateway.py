from __future__ import annotations

import os
import socket
import typing as tp

import httpx
from loguru import logger
from robonomicsinterface import RobonomicsInterface

from ._image_generation import create_qr
from .config import config
from .utils import get_headers, time_execution

IO_GATEWAY_ADDRESS: str = config.feecc_io_gateway.gateway_address
ROBONOMICS_CLIENT = RobonomicsInterface(
    seed=config.robonomics_network.account_seed,
    remote_ws=config.robonomics_network.substrate_node_url,
)


def control_flag(func: tp.Any) -> tp.Any:
    """This ensures autonomous mode is handled properly"""

    def wrap_func(*args: tp.Any, **kwargs: tp.Any) -> tp.Any:
        if config.feecc_io_gateway.autonomous_mode:
            return

        result = func(*args, **kwargs)
        return result

    return wrap_func


@control_flag
def gateway_is_up() -> None:
    """check if camera is reachable on the specified port and ip"""
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.settimeout(0.25)

    try:
        gw_socket_no_proto = IO_GATEWAY_ADDRESS.split("//")[1]
        ip, port = gw_socket_no_proto.split(":")
        s.connect((ip, int(port)))
        logger.debug(f"{IO_GATEWAY_ADDRESS} is up")
        s.close()

    except socket.error:
        raise BrokenPipeError(f"{IO_GATEWAY_ADDRESS} is unreachable")

    except Exception as e:
        logger.error(e)


@time_execution
def generate_qr_code(target_link: str) -> str:
    """generate a QR code"""
    return create_qr(target_link)


@time_execution
def post_to_datalog(content: str) -> None:
    """echo provided string to the Robonomics datalog"""
    logger.info(f"Posting data '{content}' to Robonomics datalog")
    txn_hash: str = ROBONOMICS_CLIENT.record_datalog(content)
    logger.info(f"Data '{content}' has been posted to the Robonomics datalog. {txn_hash=}")


@control_flag
@time_execution
async def publish_file(rfid_card_id: str, file_path: os.PathLike[tp.AnyStr]) -> tp.Tuple[str, str]:
    """publish a provided file to IPFS using the Feecc gateway and return it's CID and URL"""
    gateway_is_up()

    is_local_path: bool = os.path.exists(file_path)
    headers: tp.Dict[str, str] = get_headers(rfid_card_id)
    base_url = f"{IO_GATEWAY_ADDRESS}/io-gateway/publish-to-ipfs"

    async with httpx.AsyncClient(base_url=base_url, timeout=None) as client:
        if is_local_path:
            files = {"file_data": open(file_path, "rb")}
            response: httpx.Response = await client.post(url="/upload-file", headers=headers, files=files)
        else:
            json = {"absolute_path": str(file_path)}
            response = await client.post(url="/by-path", headers=headers, json=json)

    if response.is_error:
        raise httpx.RequestError(response.text)

    assert int(response.json().get("status", 500)) == 200, response.json()

    cid: str = response.json()["ipfs_cid"]
    link: str = response.json()["ipfs_link"]

    logger.info(f"File '{file_path} published to IPFS under CID {cid}'")

    return cid, link


@control_flag
@time_execution
async def print_image(file_path: str, rfid_card_id: str, annotation: tp.Optional[str] = None) -> None:
    """print the provided image file"""
    if not config.printer.enable:
        logger.warning("Printer disabled, task dropped")
        return

    gateway_is_up()

    async with httpx.AsyncClient() as client:
        url = f"{IO_GATEWAY_ADDRESS}/printing/print_image"
        headers: tp.Dict[str, str] = get_headers(rfid_card_id)
        data = {"annotation": annotation}
        files = {"image_file": open(file_path, "rb")}
        response: httpx.Response = await client.post(url=url, headers=headers, data=data, files=files)

    if response.is_error:
        raise httpx.RequestError(response.text)

    logger.info(f"Printed image '{file_path}'")
