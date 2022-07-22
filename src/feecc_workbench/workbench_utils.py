import asyncio
from collections.abc import Iterable

from ._short_url_generator import generate_short_url
from .database import MongoDbWrapper
from .Unit import Unit
from .unit_utils import UnitStatus


def _get_unit_list(unit_: Unit) -> list[Unit]:
    """list all the units in the component tree"""
    units_tree = [unit_]
    for component_ in unit_.components_units:
        nested = _get_unit_list(component_)
        units_tree.extend(nested)
    return units_tree


def determine_asssignment_target(unit: Unit, allowed_statuses: Iterable[UnitStatus]) -> tuple[Unit, bool]:
    override = unit.status == UnitStatus.built and unit.passport_ipfs_cid is None
    if unit.status in allowed_statuses or override:
        return unit, True
    return next(
        ((component, True) for component in _get_unit_list(unit) if component.status in allowed_statuses), (unit, False)
    )


def generate_short_url_background(link: str, unit_internal_id: str) -> None:
    async def _bg_generate_short_url(url: str, unit_internal_id_: str) -> None:
        short_link = await generate_short_url(url)
        await MongoDbWrapper().unit_update_single_field(unit_internal_id_, "passport_short_url", short_link)

    asyncio.create_task(_bg_generate_short_url(link, unit_internal_id))
