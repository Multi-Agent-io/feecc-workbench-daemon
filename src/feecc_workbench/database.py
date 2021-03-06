import typing as tp
from dataclasses import asdict

import pydantic
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase
from pymongo import InsertOne, UpdateOne
from yarl import URL

from .Employee import Employee
from .ProductionStage import ProductionStage
from .Singleton import SingletonMeta
from .Types import Document
from .Unit import Unit
from ._db_utils import _get_database_client, _get_unit_dict_data
from .config import CONFIG
from .exceptions import EmployeeNotFoundError, UnitNotFoundError
from .models import ProductionSchema
from .unit_utils import UnitStatus
from .utils import async_time_execution


class MongoDbWrapper(metaclass=SingletonMeta):
    """handles interactions with MongoDB database"""

    @logger.catch
    def __init__(self) -> None:
        logger.info("Trying to connect to MongoDB")

        uri = CONFIG.db.mongo_connection_uri

        self._client: AsyncIOMotorClient = _get_database_client(uri)
        db_name: str = URL(uri).path.lstrip("/")
        self._database: AsyncIOMotorDatabase = self._client[db_name]

        # collections
        self._employee_collection: AsyncIOMotorCollection = self._database.employeeData
        self._unit_collection: AsyncIOMotorCollection = self._database.unitData
        self._prod_stage_collection: AsyncIOMotorCollection = self._database.productionStagesData
        self._schemas_collection: AsyncIOMotorCollection = self._database.productionSchemas

        logger.info("Successfully connected to MongoDB")

    def close_connection(self) -> None:
        self._client.close()
        logger.info("MongoDB connection closed")

    async def _bulk_push_production_stages(self, production_stages: tp.List[ProductionStage]) -> None:
        tasks = []

        for stage in production_stages:
            if stage.is_in_db:
                task = UpdateOne({"id": stage.id}, {"$set": asdict(stage)})
            else:
                stage.is_in_db = True
                task = InsertOne(asdict(stage))

            tasks.append(task)

        result = await self._prod_stage_collection.bulk_write(tasks)
        logger.debug(f"Bulk write operation result: {result.bulk_api_result}")

    @async_time_execution
    async def upload_unit(self, unit: Unit) -> None:
        """Upload data about the unit into the DB"""
        for component in unit.components_units:
            await self.update_unit(component)

        if unit.is_in_db:
            return
        else:
            unit.is_in_db = True

        await self._bulk_push_production_stages(unit.biography)

        unit_dict = _get_unit_dict_data(unit)
        await self._unit_collection.insert_one(unit_dict)

    @async_time_execution
    async def update_unit(self, unit: Unit) -> None:
        """update data about the unit in the DB"""
        for component in unit.components_units:
            await self.update_unit(component)

        await self._bulk_push_production_stages(unit.biography)
        unit_dict = _get_unit_dict_data(unit)
        await self._unit_collection.find_one_and_update({"uuid": unit.uuid}, {"$set": unit_dict})

    @async_time_execution
    async def unit_update_single_field(self, unit_internal_id: str, field_name: str, field_val: tp.Any) -> None:
        await self._unit_collection.find_one_and_update(
            {"internal_id": unit_internal_id}, {"$set": {field_name: field_val}}
        )
        logger.debug(f"Unit {unit_internal_id} field '{field_name}' has been set to '{field_val}'")

    async def _get_unit_from_raw_db_data(self, unit_dict: Document) -> Unit:
        return Unit(
            schema=await self.get_schema_by_id(unit_dict["schema_id"]),
            uuid=unit_dict.get("uuid", None),
            internal_id=unit_dict.get("internal_id", None),
            is_in_db=unit_dict.get("is_in_db", None),
            biography=[ProductionStage(**stage) for stage in unit_dict.get("prod_stage_dicts", [])] or None,
            components_units=[
                await self.get_unit_by_internal_id(id_) for id_ in unit_dict.get("components_internal_ids", [])
            ]
            or None,
            featured_in_int_id=unit_dict.get("featured_in_int_id", None),
            passport_short_url=unit_dict.get("passport_short_url", None),
            passport_ipfs_cid=unit_dict.get("passport_ipfs_cid", None),
            txn_hash=unit_dict.get("txn_hash", None),
            serial_number=unit_dict.get("serial_number", None),
            creation_time=unit_dict.get("creation_time", None),
            status=unit_dict.get("status", None),
        )

    @async_time_execution
    async def get_unit_ids_and_names_by_status(self, status: UnitStatus) -> tp.List[tp.Dict[str, str]]:
        pipeline = [
            {"$match": {"status": status.value}},
            {
                "$lookup": {
                    "from": "productionSchemas",
                    "let": {"schema_id": "$schema_id"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$schema_id", "$$schema_id"]}}},
                        {"$project": {"_id": 0, "unit_name": 1}},
                    ],
                    "as": "unit_name",
                }
            },
            {"$unwind": {"path": "$unit_name"}},
            {"$project": {"_id": 0, "unit_name": 1, "internal_id": 1}},
        ]
        result: tp.List[Document] = await self._unit_collection.aggregate(pipeline).to_list(length=None)

        return [
            {
                "internal_id": entry["internal_id"],
                "unit_name": entry["unit_name"]["unit_name"],
            }
            for entry in result
        ]

    @async_time_execution
    async def get_employee_by_card_id(self, card_id: str) -> Employee:
        """find the employee with the provided RFID card id"""
        employee_data: tp.Optional[Document] = await self._employee_collection.find_one(
            {"rfid_card_id": card_id}, {"_id": 0}
        )

        if employee_data is None:
            message = f"No employee with card ID {card_id}"
            logger.error(message)
            raise EmployeeNotFoundError(message)

        return Employee(**employee_data)

    @async_time_execution
    async def get_unit_by_internal_id(self, unit_internal_id: str) -> Unit:
        pipeline = [
            {"$match": {"internal_id": unit_internal_id}},
            {
                "$lookup": {
                    "from": "productionStagesData",
                    "let": {"parent_uuid": "$uuid"},
                    "pipeline": [
                        {"$match": {"$expr": {"$eq": ["$parent_unit_uuid", "$$parent_uuid"]}}},
                        {"$project": {"_id": 0}},
                        {"$sort": {"number": 1}},
                    ],
                    "as": "prod_stage_dicts",
                }
            },
            {"$project": {"_id": 0}},
        ]

        try:
            result: tp.List[Document] = await self._unit_collection.aggregate(pipeline).to_list(length=1)
        except Exception as E:
            logger.error(E)
            raise E

        if not result:
            message = f"Unit with {unit_internal_id=} not found"
            logger.warning(message)
            raise UnitNotFoundError(message)

        unit_dict: Document = result[0]

        return await self._get_unit_from_raw_db_data(unit_dict)

    @async_time_execution
    async def get_all_schemas(self) -> tp.List[ProductionSchema]:
        """get all production schemas"""
        schema_data = await self._schemas_collection.find({}, {"_id": 0}).to_list(length=None)
        return [pydantic.parse_obj_as(ProductionSchema, schema) for schema in schema_data]

    @async_time_execution
    async def get_schema_by_id(self, schema_id: str) -> ProductionSchema:
        """get the specified production schema"""
        target_schema = await self._schemas_collection.find_one({"schema_id": schema_id}, {"_id": 0})

        if target_schema is None:
            raise ValueError(f"Schema {schema_id} not found")

        return pydantic.parse_obj_as(ProductionSchema, target_schema)
