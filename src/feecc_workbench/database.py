import os
import sys
import typing as tp
from dataclasses import asdict

import pydantic
from loguru import logger
from motor.motor_asyncio import AsyncIOMotorClient, AsyncIOMotorCollection, AsyncIOMotorDatabase

from .Employee import Employee
from .Singleton import SingletonMeta
from .Types import Document
from .Unit import ProductionStage, Unit
from .config import config
from .exceptions import EmployeeNotFoundError, UnitNotFoundError
from .models import ProductionSchema


def _get_database_name() -> str:
    """Get DB name in cluster from a MongoDB connection url"""
    mongo_connection_url: str = os.getenv("MONGO_CONNECTION_URL", "") or config.mongo_db.mongo_connection_url
    db_name: str = mongo_connection_url.split("/")[-1]

    if "?" in db_name:
        db_name = db_name.split("?")[0]

    return db_name


def _get_database_client() -> AsyncIOMotorClient:
    """Get MongoDB connection url"""
    mongo_connection_url: str = os.getenv("MONGO_CONNECTION_URL", "") or config.mongo_db.mongo_connection_url

    try:
        db_client = AsyncIOMotorClient(mongo_connection_url, serverSelectionTimeoutMS=3000)
        db_client.server_info()
        return db_client

    except Exception as E:
        message = (
            f"Failed to establish database connection: {E}. "
            f"Is the provided URI correct? {mongo_connection_url=} Exiting."
        )
        logger.critical(message)
        sys.exit(1)


class MongoDbWrapper(metaclass=SingletonMeta):
    """handles interactions with MongoDB database"""

    @logger.catch
    def __init__(self) -> None:
        logger.info("Trying to connect to MongoDB")

        self._client: AsyncIOMotorClient = _get_database_client()
        db_name: str = _get_database_name()
        self._database: AsyncIOMotorDatabase = self._client[db_name]

        # collections
        self._employee_collection: AsyncIOMotorCollection = self._database["Employee-data"]
        self._unit_collection: AsyncIOMotorCollection = self._database["Unit-data"]
        self._prod_stage_collection: AsyncIOMotorCollection = self._database["Production-stages-data"]
        self._schemas_collection: AsyncIOMotorCollection = self._database["Production-schemas"]

        logger.info("Successfully connected to MongoDB")

    @staticmethod
    async def _upload_dict(document: Document, collection_: AsyncIOMotorCollection) -> None:
        """insert a document into specified collection"""
        logger.debug(f"Uploading document {document} to {collection_.name}")
        await collection_.insert_one(document)

    async def _upload_dataclass(self, dataclass: tp.Any, collection_: AsyncIOMotorCollection) -> None:
        """
        convert an arbitrary dataclass to dictionary and insert it
        into the desired collection in the database
        """
        await self._upload_dict(asdict(dataclass), collection_)

    @staticmethod
    async def _find_item(key: str, value: str, collection_: AsyncIOMotorCollection) -> tp.Optional[Document]:
        """
        finds one element in the specified collection, which has
        specified key matching specified value
        """
        return await collection_.find_one({key: value}, {"_id": 0})  # type: ignore

    @staticmethod
    async def _find_many(key: str, value: str, collection_: AsyncIOMotorCollection) -> tp.List[Document]:
        """
        finds all elements in the specified collection, which have
        specified key matching specified value
        """
        return await collection_.find({key: value}, {"_id": 0}).to_list(length=None)  # type: ignore

    @staticmethod
    async def _get_all_items_in_collection(collection_: AsyncIOMotorCollection) -> tp.List[Document]:
        """get all documents in the provided collection"""
        return await collection_.find({}, {"_id": 0}).to_list(length=None)  # type: ignore

    @staticmethod
    async def _update_document(
        key: str, value: str, new_document: Document, collection_: AsyncIOMotorCollection
    ) -> None:
        """
        finds matching document in the specified collection, and replaces it's data
        with what is provided in the new_document argument
        """
        logger.debug(f"Updating key {key} with value {value}")
        await collection_.find_one_and_update({key: value}, {"$set": new_document})

    async def update_production_stage(self, updated_production_stage: ProductionStage) -> None:
        """update data about the production stage in the DB"""
        stage_dict: Document = asdict(updated_production_stage)
        stage_id: str = updated_production_stage.id
        await self._update_document("id", stage_id, stage_dict, self._prod_stage_collection)

    async def update_unit(self, unit: Unit) -> None:
        """update data about the unit in the DB"""
        for stage in unit.biography:
            if stage.is_in_db:
                await self.update_production_stage(stage)
            else:
                await self.upload_production_stage(stage)

        unit_dict = unit.dict_data()
        await self._update_document("uuid", unit.uuid, unit_dict, self._unit_collection)

    async def upload_unit(self, unit: Unit) -> None:
        """
        convert a unit instance into a dictionary suitable for future reassembly while
        converting nested structures and uploading them
        """
        for component in unit.components_units:
            await self.upload_unit(component)

        if unit.is_in_db:
            return
        else:
            unit.is_in_db = True

        unit_dict = unit.dict_data()

        # upload nested dataclasses
        for stage in unit.biography:
            await self.upload_production_stage(stage)

        await self._upload_dict(unit_dict, self._unit_collection)

    async def upload_production_stage(self, production_stage: ProductionStage) -> None:
        if production_stage.is_in_db:
            return

        production_stage.is_in_db = True
        await self._upload_dataclass(production_stage, self._prod_stage_collection)

    async def get_employee_by_card_id(self, card_id: str) -> Employee:
        """find the employee with the provided RFID card id"""
        employee_data: tp.Optional[Document] = await self._find_item("rfid_card_id", card_id, self._employee_collection)

        if employee_data is None:
            message = f"No employee with card ID {card_id}"
            logger.error(message)
            raise EmployeeNotFoundError(message)

        return Employee(**employee_data)

    async def get_unit_by_internal_id(self, unit_internal_id: str) -> Unit:
        try:
            unit_dict: Document = await self._find_item("internal_id", unit_internal_id, self._unit_collection)  # type: ignore
            if unit_dict is None:
                raise ValueError("Unit not found")
            prod_stage_dicts = await self._find_many("parent_unit_uuid", unit_dict["uuid"], self._prod_stage_collection)
            prod_stage_dicts.sort(key=lambda d: d.get("number", 0))
            return Unit(
                schema=await self.get_schema_by_id(unit_dict["schema_id"]),
                uuid=unit_dict.get("uuid", None),
                internal_id=unit_dict.get("internal_id", None),
                is_in_db=unit_dict.get("is_in_db", None),
                biography=[ProductionStage(**stage) for stage in prod_stage_dicts] or None,
                components_units=[
                    await self.get_unit_by_internal_id(id_) for id_ in unit_dict.get("components_internal_ids", [])
                ]
                or None,
                passport_short_url=unit_dict.get("passport_short_url", None),
                featured_in_int_id=unit_dict.get("featured_in_int_id", None),
            )

        except Exception as E:
            logger.error(E)
            message: str = f"Could not find the Unit with int. id {unit_internal_id}. Does it exist?"
            raise UnitNotFoundError(message)

    async def get_all_schemas(self) -> tp.List[ProductionSchema]:
        """get all production schemas"""
        schema_data = await self._get_all_items_in_collection(self._schemas_collection)
        return [pydantic.parse_obj_as(ProductionSchema, schema) for schema in schema_data]

    async def get_schema_by_id(self, schema_id: str) -> ProductionSchema:
        """get the specified production schema"""
        target_schema = await self._find_item("schema_id", schema_id, self._schemas_collection)

        if target_schema is None:
            raise ValueError(f"Schema {schema_id} not found")

        return pydantic.parse_obj_as(ProductionSchema, target_schema)
