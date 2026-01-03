from fastapi import Depends

from .db import get_db
from .services.data_service import DataService
from .services.ingestion_service import IngestionService


async def get_data_service(db=Depends(get_db)) -> DataService:
    """Get DataService instance. Indexes are ensured at startup."""
    return DataService(db)


async def get_ingestion_service(data_service=Depends(get_data_service)) -> IngestionService:
    """Get IngestionService instance."""
    return IngestionService(data_service)
