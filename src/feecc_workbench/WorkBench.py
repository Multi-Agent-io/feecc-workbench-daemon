import asyncio
import os
from pathlib import Path

from loguru import logger

from ._image_generation import create_qr, create_seal_tag
from ._short_url_generator import generate_short_url
from .Camera import Camera
from .config import CONFIG
from .database import MongoDbWrapper
from .Employee import Employee
from .exceptions import StateForbiddenError
from .ipfs import publish_file
from .Messenger import messenger
from .models import ProductionSchema
from .passport_generator import construct_unit_passport
from .printer import print_image
from .robonomics import post_to_datalog
from .Singleton import SingletonMeta
from .states import STATE_TRANSITION_MAP, State
from .Types import AdditionalInfo
from .Unit import Unit
from .unit_utils import UnitStatus
from .utils import timestamp

STATE_SWITCH_EVENT = asyncio.Event()


class WorkBench(metaclass=SingletonMeta):
    """
    Work bench is a union of an Employee, working at it and Camera attached.
    It provides highly abstract interface for interaction with them
    """

    @logger.catch
    def __init__(self) -> None:
        self._database: MongoDbWrapper = MongoDbWrapper()
        self.number: int = CONFIG.workbench.number
        camera_number: int | None = CONFIG.camera.camera_no
        self.camera: Camera | None = Camera(camera_number) if camera_number and CONFIG.camera.enable else None
        self.employee: Employee | None = None
        self.unit: Unit | None = None
        self.state: State = State.AWAIT_LOGIN_STATE

        logger.info(f"Workbench {self.number} was initialized")

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    async def create_new_unit(self, schema: ProductionSchema) -> Unit:
        """initialize a new instance of the Unit class"""
        if self.state != State.AUTHORIZED_IDLING_STATE:
            message = "Cannot create a new unit unless workbench has state AuthorizedIdling"
            messenger.error("Для создания нового изделия рабочий стол должен иметь состояние AuthorizedIdling")
            raise StateForbiddenError(message)

        unit = Unit(schema)

        if CONFIG.printer.print_barcode and CONFIG.printer.enable:
            if unit.schema.parent_schema_id is None:
                annotation = unit.schema.unit_name
            else:
                parent_schema = await self._database.get_schema_by_id(unit.schema.parent_schema_id)
                annotation = f"{parent_schema.unit_name}. {unit.model_name}."

            assert self.employee is not None

            try:
                await print_image(Path(unit.barcode.filename), self.employee.rfid_card_id, annotation=annotation)
            except Exception as e:
                messenger.error(f"Ошибка при печати этикетки: {e}")
                raise e
            finally:
                os.remove(unit.barcode.filename)

        await self._database.push_unit(unit)

        return unit

    def _validate_state_transition(self, new_state: State) -> None:
        """check if state transition can be performed using the map"""
        if new_state not in STATE_TRANSITION_MAP.get(self.state, []):
            message = f"State transition from {self.state.value} to {new_state.value} is not allowed."
            messenger.error(f"Переход из состояния {self.state.value} в состояние {new_state.value} невозможен")
            raise StateForbiddenError(message)

    def switch_state(self, new_state: State) -> None:
        """apply new state to the workbench"""
        assert isinstance(new_state, State)
        self._validate_state_transition(new_state)
        logger.info(f"Workbench no.{self.number} state changed: {self.state.value} -> {new_state.value}")
        self.state = new_state
        STATE_SWITCH_EVENT.set()

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    def log_in(self, employee: Employee) -> None:
        """authorize employee"""
        self._validate_state_transition(State.AUTHORIZED_IDLING_STATE)

        self.employee = employee
        message = f"Employee {employee.name} is logged in at the workbench no. {self.number}"
        logger.info(message)
        messenger.success(f"Авторизован {employee.position} {employee.name}")

        self.switch_state(State.AUTHORIZED_IDLING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    def log_out(self) -> None:
        """log out the employee"""
        self._validate_state_transition(State.AWAIT_LOGIN_STATE)

        if self.state == State.UNIT_ASSIGNED_IDLING_STATE:
            self.remove_unit()

        assert self.employee is not None
        message = f"Employee {self.employee.name} was logged out at the workbench no. {self.number}"
        logger.info(message)
        messenger.success(f"{self.employee.name} вышел из системы")
        self.employee = None

        self.switch_state(State.AWAIT_LOGIN_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    def assign_unit(self, unit: Unit) -> None:
        """assign a unit to the workbench"""
        self._validate_state_transition(State.UNIT_ASSIGNED_IDLING_STATE)

        def _get_unit_list(unit_: Unit) -> list[Unit]:
            """list all the units in the component tree"""
            units_tree = [unit_]
            for component_ in unit_.components_units:
                nested = _get_unit_list(component_)
                units_tree.extend(nested)
            return units_tree

        allowed = (UnitStatus.production, UnitStatus.revision)
        override = unit.status == UnitStatus.built and unit.passport_ipfs_cid is None

        if unit.status not in allowed:
            for component in _get_unit_list(unit):
                if component.status in allowed:
                    unit = component
                    break

        if not (override or unit.status in allowed):
            message = f"Can only assign unit with status: {', '.join(s.value for s in allowed)}. Unit status is {unit.status.value}. Forbidden."
            messenger.warning(
                f"На стол могут быть помещены изделия со статусами:"
                f" {', '.join(s.value.upper() for s in allowed)}."
                f" Статус изделия: {unit.status.value.upper()}. Отказано."
            )
            raise AssertionError(message)

        self.unit = unit

        message = f"Unit {unit.internal_id} has been assigned to the workbench"
        logger.info(message)
        messenger.success(f"Изделие с внутренним номером {unit.internal_id} помещено на стол")

        if not unit.components_filled:
            logger.info(
                f"Unit {unit.internal_id} is a composition with unsatisfied component requirements. Entering component gathering state."
            )
            self.switch_state(State.GATHER_COMPONENTS_STATE)
        else:
            self.switch_state(State.UNIT_ASSIGNED_IDLING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    def remove_unit(self) -> None:
        """remove a unit from the workbench"""
        self._validate_state_transition(State.AUTHORIZED_IDLING_STATE)

        if self.unit is None:
            message = "Cannot remove unit. No unit is currently assigned to the workbench."
            messenger.error("Невозможно убрать со стола изделие. На рабочем столе отсутсвует изделие")
            raise AssertionError(message)

        message = f"Unit {self.unit.internal_id} has been removed from the workbench"
        logger.info(message)
        messenger.success(f"Изделие с внутренним номером {self.unit.internal_id} убрано со стола")

        self.unit = None

        self.switch_state(State.AUTHORIZED_IDLING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    async def start_operation(self, additional_info: AdditionalInfo) -> None:
        """begin work on the provided unit"""
        self._validate_state_transition(State.PRODUCTION_STAGE_ONGOING_STATE)

        if self.unit is None:
            message = "No unit is assigned to the workbench"
            messenger.error("На рабочем столе отсутсвует изделие")
            raise AssertionError(message)

        if self.employee is None:
            message = "No employee is logged in at the workbench"
            messenger.error("Необходима авторизация")
            raise AssertionError(message)

        if self.camera is not None:
            await self.camera.start(self.employee.rfid_card_id)

        self.unit.start_operation(self.employee, additional_info)

        self.switch_state(State.PRODUCTION_STAGE_ONGOING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError, ValueError))
    async def assign_component_to_unit(self, component: Unit) -> None:
        """assign provided component to a composite unit"""
        assert (
            self.state == State.GATHER_COMPONENTS_STATE and self.unit is not None
        ), f"Cannot assign components unless WB is in state {State.GATHER_COMPONENTS_STATE}"

        self.unit.assign_component(component)
        STATE_SWITCH_EVENT.set()

        if self.unit.components_filled:
            await self._database.push_unit(self.unit)
            self.switch_state(State.UNIT_ASSIGNED_IDLING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    async def end_operation(self, additional_info: AdditionalInfo | None = None, premature: bool = False) -> None:
        """end work on the provided unit"""
        self._validate_state_transition(State.UNIT_ASSIGNED_IDLING_STATE)

        if self.unit is None:
            message = "No unit is assigned to the workbench"
            messenger.error("На рабочем столе отсутсвует изделие")
            raise AssertionError(message)

        logger.info("Trying to end operation")
        override_timestamp = timestamp()
        ipfs_hashes: list[str] = []

        if self.camera is not None and self.employee is not None:
            try:
                await self.camera.end(self.employee.rfid_card_id)
                override_timestamp = timestamp()
                assert self.camera.record is not None, "No record found"
                file: str | None = self.camera.record.remote_file_path
            except Exception as e:
                logger.error(f"Failed to end record: {e}")
                messenger.warning("Этап завершен, однако сохранить видео не удалось. Обратитесь к администратору.")
                file = None

            if file is not None:
                try:
                    data = await publish_file(file_path=Path(file), rfid_card_id=self.employee.rfid_card_id)

                    if data is not None:
                        cid, link = data
                        ipfs_hashes.append(cid)
                except Exception as e:
                    logger.error(f"Failed to publish record: {e}")
                    messenger.warning(
                        "Этап завершен, однако опубликовать видеозапись в сети IPFS не удалось. "
                        "Видеозапись сохранена локально. Обратитесь к администратору."
                    )
                    ipfs_hashes = []

        self.unit.end_operation(
            video_hashes=ipfs_hashes,
            additional_info=additional_info,
            premature=premature,
            override_timestamp=override_timestamp,
        )
        await self._database.push_unit(self.unit, include_components=False)

        self.switch_state(State.UNIT_ASSIGNED_IDLING_STATE)

    @logger.catch(reraise=True, exclude=(StateForbiddenError, AssertionError))
    async def upload_unit_passport(self) -> None:
        """upload passport file into IPFS and pin it to Pinata, publish hash to Robonomics"""
        if self.unit is None:
            message = "No unit is assigned to the workbench"
            messenger.error("На рабочем столе отсутсвует изделие")
            raise AssertionError(message)

        if self.employee is None:
            message = "No employee is logged in at the workbench"
            messenger.error("Необходима авторизация")
            raise AssertionError(message)

        passport_file_path: Path = await construct_unit_passport(self.unit)

        if CONFIG.ipfs_gateway.enable:
            res = await publish_file(file_path=passport_file_path, rfid_card_id=self.employee.rfid_card_id)
            cid, link = res
            self.unit.passport_ipfs_cid = cid

            print_qr = CONFIG.printer.print_qr and (
                not CONFIG.printer.print_qr_only_for_composite
                or self.unit.schema.is_composite
                or not self.unit.schema.is_a_component
            )

            if print_qr:
                short_url: str = await generate_short_url(link)
                self.unit.passport_short_url = short_url
                qrcode_path = create_qr(short_url)
                try:
                    if self.unit.schema.parent_schema_id is None:
                        annotation = f"{self.unit.model_name} (ID: {self.unit.internal_id}). {short_url}"
                    else:
                        parent_schema = await self._database.get_schema_by_id(self.unit.schema.parent_schema_id)
                        annotation = f"{parent_schema.unit_name}. {self.unit.model_name} (ID: {self.unit.internal_id}). {short_url}"

                    await print_image(
                        qrcode_path,
                        self.employee.rfid_card_id,
                        annotation=annotation,
                    )
                except Exception as e:
                    messenger.error(f"Ошибка при печати QR-кода: {e}")
                finally:
                    os.remove(qrcode_path)
            else:

                async def _bg_generate_short_url(url: str, unit_internal_id: str) -> None:
                    short_link = await generate_short_url(url)
                    await MongoDbWrapper().unit_update_single_field(unit_internal_id, "passport_short_url", short_link)

                asyncio.create_task(_bg_generate_short_url(link, self.unit.internal_id))

            if CONFIG.printer.print_security_tag:
                seal_tag_img: Path = create_seal_tag()

                try:
                    await print_image(seal_tag_img, self.employee.rfid_card_id)
                except Exception as e:
                    messenger.error(f"Ошибка при печати пломбы: {e}")
                finally:
                    os.remove(seal_tag_img)

            if CONFIG.robonomics.enable_datalog and res is not None:
                asyncio.create_task(post_to_datalog(cid, self.unit.internal_id))

        await self._database.push_unit(self.unit)

    async def shutdown(self) -> None:
        logger.info("Workbench shutdown sequence initiated")
        messenger.warning("Завершение работы сервера. Не выключайте машину!")

        if self.state == State.PRODUCTION_STAGE_ONGOING_STATE:
            logger.warning(
                "Ending ongoing operation prematurely. Reason: Unfinished when Workbench shutdown sequence initiated"
            )
            await self.end_operation(
                premature=True,
                additional_info={"Ended reason": "Unfinished when Workbench shutdown sequence initiated"},
            )

        if self.state in (State.UNIT_ASSIGNED_IDLING_STATE, State.GATHER_COMPONENTS_STATE):
            self.remove_unit()

        if self.state == State.AUTHORIZED_IDLING_STATE:
            self.log_out()

        message = "Workbench shutdown sequence complete"
        logger.info(message)
        messenger.success("Работа сервера завершена")
