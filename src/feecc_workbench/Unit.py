from __future__ import annotations

import datetime as dt
from functools import reduce
from operator import add
from typing import no_type_check
from uuid import uuid4

from loguru import logger

from ._Barcode import Barcode
from .Employee import Employee
from .Messenger import messenger
from .models import ProductionSchema
from .ProductionStage import ProductionStage
from .Types import AdditionalInfo
from .unit_utils import UnitStatus, biography_factory
from .utils import TIMESTAMP_FORMAT, timestamp


class Unit:
    """Unit class corresponds to one uniquely identifiable physical production unit"""

    def __init__(
        self,
        schema: ProductionSchema,
        uuid: str | None = None,
        internal_id: str | None = None,
        is_in_db: bool | None = None,
        biography: list[ProductionStage] | None = None,
        components_units: list[Unit] | None = None,
        featured_in_int_id: str | None = None,
        passport_short_url: str | None = None,
        passport_ipfs_cid: str | None = None,
        txn_hash: str | None = None,
        serial_number: str | None = None,
        creation_time: dt.datetime | None = None,
        status: UnitStatus | str = UnitStatus.production,
    ) -> None:
        self.status: UnitStatus = UnitStatus(status) if isinstance(status, str) else status

        if not schema.production_stages and self.status is UnitStatus.production:
            self.status = UnitStatus.built

        self.schema: ProductionSchema = schema
        self.uuid: str = uuid or uuid4().hex
        self.barcode: Barcode = Barcode(str(int(self.uuid, 16))[:12])
        self.internal_id: str = internal_id or str(self.barcode.barcode.get_fullcode())
        self.passport_short_url: str | None = passport_short_url
        self.passport_ipfs_cid: str | None = passport_ipfs_cid
        self.txn_hash: str | None = txn_hash
        self.serial_number: str | None = serial_number
        self.components_units: list[Unit] = components_units or []
        self.featured_in_int_id: str | None = featured_in_int_id
        self.employee: Employee | None = None
        self.biography: list[ProductionStage] = biography or biography_factory(schema, self.uuid)
        self.is_in_db: bool = is_in_db or False
        self.creation_time: dt.datetime = creation_time or dt.datetime.now()

        slots: dict[str, Unit | None] = {schema_id: None for schema_id in (schema.required_components_schema_ids or [])}
        for component in self.components_units:
            if component.schema.schema_id in slots:
                slots[component.schema.schema_id] = component

        self._component_slots: dict[str, Unit | None] = slots

    @property
    def components_schema_ids(self) -> list[str]:
        return self.schema.required_components_schema_ids or []

    @property
    def components_internal_ids(self) -> list[str]:
        return [c.internal_id for c in self.components_units]

    @property
    def model_name(self) -> str:
        return self.schema.unit_name

    @property
    def components_filled(self) -> bool:
        return None not in self._component_slots.values()

    @property
    def next_pending_operation(self) -> ProductionStage | None:
        """get next pending operation if any"""
        return next((operation for operation in self.biography if not operation.completed), None)

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

    @no_type_check
    def assigned_components(self) -> dict[str, str | None] | None:
        """get a mapping for all the currently assigned components VS the desired components"""
        assigned_components = {component.schema.schema_id: component.internal_id for component in self.components_units}

        for component_name in self.components_schema_ids:
            if component_name not in assigned_components:
                assigned_components[component_name] = None

        return assigned_components or None

    def assign_component(self, component: Unit) -> None:
        """Assign one of the composite unit's components to the unit"""
        if self.components_filled:
            messenger.warning("Изделию уже присвоены все необходимые компоненты")
            raise ValueError(f"Unit {self.model_name} component requirements have already been satisfied")

        if component.schema.schema_id not in self._component_slots:
            messenger.warning(f'Комопнент "{component.model_name}" не явлеяется частью изделия "{self.model_name}"')
            raise ValueError(
                f"Cannot assign component {component.model_name} to {self.model_name} as it's not a component of it"
            )

        if self._component_slots.get(component.schema.schema_id, "") is not None:
            messenger.warning(f'Компонент "{component.model_name}" уже был добавлен к этому изделию')
            raise ValueError(
                f"Component {component.model_name} is already assigned to a composite Unit {self.model_name}"
            )

        if component.status is not UnitStatus.built:
            messenger.warning(
                f'Сборка компонента "{component.model_name}" не была завершена. Невозможно присвоить компонент'
            )
            raise ValueError(f"Component {component.model_name} assembly is not completed. {component.status=}")

        if component.featured_in_int_id is not None:
            messenger.warning(
                f"Компонент №{component.internal_id} уже использован в изделии №{component.featured_in_int_id}"
            )
            raise ValueError(
                f"Component {component.model_name} has already been used in unit {component.featured_in_int_id}"
            )

        self._component_slots[component.schema.schema_id] = component
        self.components_units.append(component)
        component.featured_in_int_id = self.internal_id
        logger.info(f"Component {component.model_name} has been assigned to a composite Unit {self.model_name}")
        messenger.success(f'Компонент "{component.model_name}" присвоен изделию "{self.model_name}"')

    def start_operation(
        self,
        employee: Employee,
        additional_info: AdditionalInfo | None = None,
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

    def end_operation(
        self,
        video_hashes: list[str] | None = None,
        additional_info: AdditionalInfo | None = None,
        premature: bool = False,
        override_timestamp: str | None = None,
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
        operation.video_hashes = video_hashes
        operation.completed = True

        if premature:
            self._duplicate_current_operation()
            operation.name += " (неокончен.)"
            operation.ended_prematurely = True

        if additional_info:
            operation.additional_info = {
                **(operation.additional_info or {}),
                **additional_info,
            }

        self.biography[operation.number] = operation

        if all(stage.completed for stage in self.biography):
            prev_status = self.status
            self.status = UnitStatus.built
            logger.info(
                f"Unit has no more pending production stages. Unit status changed: {prev_status.value} -> "
                f"{self.status.value}"
            )

        self.employee = None
