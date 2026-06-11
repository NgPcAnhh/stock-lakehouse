import uuid
import asyncpg
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select
from app.modules.bi.models.data_source import DataSource
from app.modules.bi.data_sources.schemas import DataSourceCreate, DataSourceUpdate, TestConnectionRequest
from app.core.config import get_settings
from cryptography.fernet import Fernet

from app.modules.bi.data_sources.engines import get_engine

settings = get_settings()
encryption_key = getattr(settings, "ENCRYPTION_KEY", "FW9yUp0yRl1lVuUv6ZuWbXX4SMWcheXrysMpQ_o8xxM=")
fernet = Fernet(encryption_key.encode('utf-8') if len(encryption_key) == 44 else Fernet.generate_key())

def encrypt_password(password: str) -> str:
    return fernet.encrypt(password.encode('utf-8')).decode('utf-8')

def decrypt_password(encrypted_password: str) -> str:
    try:
        return fernet.decrypt(encrypted_password.encode('utf-8')).decode('utf-8')
    except Exception as e:
        print(f"Failed to decrypt password: {e}")
        return ""

async def get_data_sources(db: AsyncSession, workspace_id: uuid.UUID):
    query = select(DataSource).where(DataSource.workspace_id == workspace_id)
    result = await db.execute(query)
    return result.scalars().all()

async def create_data_source(db: AsyncSession, data_source_in: DataSourceCreate):
    db_obj = DataSource(
        workspace_id=data_source_in.workspace_id,
        name=data_source_in.name,
        type=data_source_in.type,
        host=data_source_in.host,
        port=data_source_in.port,
        database_name=data_source_in.database_name,
        username=data_source_in.username,
        ssl_config=data_source_in.ssl_config,
        extra_config=data_source_in.extra_config
    )
    if data_source_in.password:
        db_obj.encrypted_password = encrypt_password(data_source_in.password)
    
    db.add(db_obj)
    await db.commit()
    await db.refresh(db_obj)
    return db_obj

async def test_connection(req: TestConnectionRequest) -> bool:
    try:
        engine = get_engine(req.type)
        config = {
            'host': req.host,
            'port': req.port,
            'username': req.username,
            'password': req.password,
            'database_name': req.database_name
        }
        return await engine.test_connection(config)
    except Exception as e:
        print(f"Connection failed: {e}")
        return False

async def update_data_source(db: AsyncSession, db_obj: DataSource, obj_in: DataSourceUpdate) -> DataSource:
    update_data = obj_in.model_dump(exclude_unset=True)
    if "password" in update_data:
        if update_data["password"]:
            db_obj.encrypted_password = encrypt_password(update_data["password"])
        del update_data["password"]
    
    for field in update_data:
        setattr(db_obj, field, update_data[field])
        
    db.add(db_obj)
    await db.commit()
    await db.refresh(db_obj)
    return db_obj

async def delete_data_source(db: AsyncSession, db_obj: DataSource) -> None:
    await db.delete(db_obj)
    await db.commit()

async def get_databases(db: AsyncSession, db_obj: DataSource) -> list[str]:
    engine = get_engine(db_obj.type)
    password = decrypt_password(db_obj.encrypted_password) if db_obj.encrypted_password else None
    config = {
        'host': db_obj.host,
        'port': db_obj.port,
        'username': db_obj.username,
        'password': password,
        'database_name': db_obj.database_name
    }
    return await engine.get_databases(config)

async def get_database_metadata(db: AsyncSession, db_obj: DataSource, target_db: str = None) -> dict:
    engine = get_engine(db_obj.type)
    password = decrypt_password(db_obj.encrypted_password) if db_obj.encrypted_password else None
    config = {
        'host': db_obj.host,
        'port': db_obj.port,
        'username': db_obj.username,
        'password': password,
        'database_name': db_obj.database_name
    }
    return await engine.get_metadata(config, target_db=target_db)

