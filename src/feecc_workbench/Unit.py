from __future__ import annotations

import datetime as dt
import typing as tp
from functools import reduce
from operator import add
from uuid import uuid4

from loguru import logger

from .Employee import Employee
from .ProductionStage import ProductionStage
from .Types import AdditionalInfo
from ._Barcode import Barcode
from .models import ProductionSchema
from .unit_utils import UnitStatus, biography_factory
from .utils import TIMESTAMP_FORMAT, timestamp


class Unit:
    """Unit class corresponds to one uniquely identifiable physical production unit"""

    def __init__(
        self,
        schema: ProductionSchema,
        uuid: tp.Optional[str] = None,
        internal_id: tp.Optional[str] = None,
        is_in_db: tp.Optional[bool] = None,
        biography: tp.Optional[tp.List[ProductionStage]] = None,
        components_units: tp.Optional[tp.List[Unit]] = None,
        featured_in_int_id: tp.Optional[str] = None,
        passport_short_url: tp.Optional[str] = None,
        passport_ipfs_cid: tp.Optional[str] = None,
        txn_hash: tp.Optional[str] = None,
        serial_number: tp.Optional[str] = None,
        creation_time: tp.Optional[dt.datetime] = None,
        status: tp.Union[UnitStatus, str] = UnitStatus.production,
    ) -> None:
        self.status: UnitStatus = UnitStatus(status) if isinstance(status, str) else status

        if not schema.production_stages and self.status is UnitStatus.production:
            self.status = UnitStatus.built

        self.schema: ProductionSchema = schema
        self.uuid: str = uuid or uuid4().hex
        self.barcode: Barcode = Barcode(str(int(self.uuid, 16))[:12])
        self.internal_id: str = internal_id or str(self.barcode.barcode.get_fullcode())
        self.passport_short_url: tp.Optional[str] = passport_short_url
        self.passport_ipfs_cid: tp.Optional[str] = passport_ipfs_cid
        self.txn_hash: tp.Optional[str] = txn_hash
        self.serial_number: tp.Optional[str] = serial_number
        self.components_units: tp.List[Unit] = components_units or []
        self.featured_in_int_id: tp.Optional[str] = featured_in_int_id
        self.employee: tp.Optional[Employee] = None
        self.biography: tp.List[ProductionStage] = biography or biography_factory(schema, self.uuid)
        self.is_in_db: bool = is_in_db or False
        self.creation_time: dt.datetime = creation_time or dt.datetime.now()

    @property
    def components_schema_ids(self) -> tp.List[str]:
        return self.schema.required_components_schema_ids or []

    @property
    def components_internal_ids(self) -> tp.List[str]:
        return [c.internal_id for c in self.components_units]

    @property
    def model_name(self) -> str:
        return self.schema.unit_name

    @property
    def components_filled(self) -> bool:
        if self.components_schema_ids:
            if not self.components_units:
                return False

            return len(self.components_schema_ids) == len(self.components_units)

        return True

    @property
    def next_pending_operation(self) -> tp.Optional[ProductionStage]:
        """get next pending operation if any"""
        for operation in self.biography:
            if not operation.completed:
                return operation

        return None

    @property
    def total_assembly_time(self) -> dt.timedelta:
        """calculate total time spent during all production stages"""

        def stage_len(stage: ProductionStage) -> dt.timedelta:
            if stage.session_start_time is None:
                return dt.timedelta(0)

            start_time: dt.datetime = dt.datetime.strptime(stage.session_start_time, TIMESTAMP_FORMAT)
            end_time: dt.datetime = (
                dt.datetime.strptime(stage.session_end_time, TIMESTAMP_FORMAT)
                if stage.session_end_time is not None
                else dt.datetime.now()
            )
            return end_time - start_time

        return reduce(add, (stage_len(stage) for stage in self.biography)) if self.biography else dt.timedelta(0)

    @tp.no_type_check
    def assigned_components(self) -> tp.Optional[tp.Dict[str, tp.Optional[str]]]:
        """get a mapping for all the currently assigned components VS the desired components"""
        assigned_components = {component.schema.schema_id: component.internal_id for component in self.components_units}

        for component_name in self.components_schema_ids:
            if component_name not in assigned_components:
                assigned_components[component_name] = None

        return assigned_components or None

    def assign_component(self, component: Unit) -> None:
        """acquire one of the composite unit's components"""
        if self.components_filled:
            logger.error(f"Unit {self.model_name} component requirements have already been satisfied")

        elif component.schema.schema_id in self.components_schema_ids:
            if component.schema.schema_id not in (c.schema.schema_id for c in self.components_units):
                if component.status is not UnitStatus.built:
                    raise ValueError(f"Component {component.model_name} assembly is not completed. {component.status=}")

                elif component.featured_in_int_id is not None:
                    raise ValueError(
                        f"Component {component.model_name} has already been used in unit {component.featured_in_int_id}"
                    )

                else:
                    self.components_units.append(component)
                    component.featured_in_int_id = self.internal_id
                    logger.info(
                        f"Component {component.model_name} has been assigned to a composite Unit {self.model_name}"
                    )

            else:
                message = f"Component {component.model_name} is already assigned to a composite Unit {self.model_name}"
                logger.error(message)
                raise ValueError(message)

        else:
            message = (
                f"Cannot assign component {component.model_name} to {self.model_name} as it's not a component of it"
            )
            logger.error(message)
            raise ValueError(message)

    def start_operation(
        self,
        employee: Employee,
        additional_info: tp.Optional[AdditionalInfo] = None,
    ) -> None:
        """begin the provided operation and save data about it"""
        operation = self.next_pending_operation
        assert operation is not None, f"Unit {self.uuid} has no pending operations ({self.status=})"
        operation.session_start_time = timestamp()
        operation.additional_info = additional_info
        operation.employee_name = employee.passport_code
        self.biography[operation.number] = operation
        logger.debug(f"Started production stage {operation.name} for unit {self.uuid}")

    def _duplicate_current_operation(self) -> None:
        cur_stage = self.next_pending_operation
        assert cur_stage is not None, "No pending stages to duplicate"
        target_pos = cur_stage.number + 1
        dup_operation = ProductionStage(
            name=cur_stage.name,
            parent_unit_uuid=cur_stage.parent_unit_uuid,
            number=target_pos,
            schema_stage_id=cur_stage.schema_stage_id,
        )
        self.biography.insert(target_pos, dup_operation)

        for i in range(target_pos + 1, len(self.biography)):
            self.biography[i].number += 1

    async def end_operation(
        self,
        video_hashes: tp.Optional[tp.List[str]] = None,
        additional_info: tp.Optional[AdditionalInfo] = None,
        premature: bool = False,
        override_timestamp: tp.Optional[str] = None,
    ) -> None:
        """
        wrap up the session when video recording stops and save video data
        as well as session end timestamp
        """
        operation = self.next_pending_operation

        if operation is None:
            raise ValueError("No pending operations found")

        logger.info(f"Ending production stage {operation.name} on unit {self.uuid}")
        operation.session_end_time = override_timestamp or timestamp()

        if premature:
            self._duplicate_current_operation()
            operation.name += " (??????????????????.)"
            operation.ended_prematurely = True

        if video_hashes:
            operation.video_hashes = video_hashes

        if additional_info:
            if operation.additional_info is not None:
                operation.additional_info = {
                    **operation.additional_info,
                    **additional_info,
                }
            else:
                operation.additional_info = additional_info

        operation.completed = True
        self.biography[operation.number] = operation

        if all(stage.completed for stage in self.biography):
            prev_status = self.status
            self.status = UnitStatus.built
            logger.info(
                f"Unit has no more pending production stages. Unit status changed: {prev_status.value} -> "
                f"{self.status.value}"
            )

        self.employee = None
