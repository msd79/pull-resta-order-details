import os
from pathlib import Path
from typing import List, Optional
from dataclasses import dataclass
import yaml
import logging

logger = logging.getLogger(__name__)

@dataclass
class APIConfig:
    base_url: str
    page_size: int
    request_timeout: int
    api_client: int = 1
    api_client_version: int = 196

    @property
    def detail_url(self) -> str:
        return f"{self.base_url}/order/Detail"

@dataclass
class DatabaseConfig:
    server: str
    database: str
    username: str
    driver: str
    port: int = 1433

    @property
    def password(self) -> str:
        return os.getenv('DB_PASSWORD')
    
    @property
    def passphrase(self) -> str:
        return os.getenv('DB_PASSPHRASE')

    @property
    def connection_string(self) -> str:
        """
        Creates a SQLAlchemy connection string with proper encoding for special characters.
        """
        from urllib.parse import quote_plus
        
        # Handle server name with instance
        server = self.server.replace('\\', '\\\\')
        
        # URL encode the driver and password
        encoded_driver = quote_plus(self.driver)
        encoded_password = quote_plus(self.password) if self.password else ''
        
        #return f"mssql+pyodbc://{self.username}:{encoded_password}@{server}/{self.database}?driver={encoded_driver}"
   
        server_with_port = f"{server}:{self.port}"
        
        return f"mssql+pyodbc://{self.username}:{encoded_password}@{server_with_port}/{self.database}?driver={encoded_driver}"

    @staticmethod
    def get_available_drivers() -> list:
        """
        Returns a list of available ODBC drivers on the system.
        """
        import pyodbc
        return pyodbc.drivers()


@dataclass
class LoggingConfig:
    filename: str
    level: str
    max_bytes: int
    backup_count: int

@dataclass
class SyncConfig:
    polling_interval: int
    request_delay: float
    max_retries: int
    delay_between_orders: float
    delay_between_pages: float
    delay_on_error: float
@dataclass
class ScheduleConfig:
    """Configuration for application running schedule"""
    start_hour: int
    start_minute: int
    end_hour: int
    end_minute: int
    active_days: List[str]

    def __post_init__(self):
        """Validate schedule configuration"""
        if not 0 <= self.start_hour <= 23:
            raise ValueError(f"Invalid start_hour: {self.start_hour}")
        if not 0 <= self.end_hour <= 23:
            raise ValueError(f"Invalid end_hour: {self.end_hour}")
        if not 0 <= self.start_minute <= 59:
            raise ValueError(f"Invalid start_minute: {self.start_minute}")
        if not 0 <= self.end_minute <= 59:
            raise ValueError(f"Invalid end_minute: {self.end_minute}")
        
        valid_days = {'MONDAY', 'TUESDAY', 'WEDNESDAY', 'THURSDAY', 'FRIDAY', 'SATURDAY', 'SUNDAY'}
        for day in self.active_days:
            if day not in valid_days:
                raise ValueError(f"Invalid day: {day}. Must be one of {valid_days}")

@dataclass
class Config:
    api: APIConfig
    database: DatabaseConfig
    logging: LoggingConfig
    sync: SyncConfig
    schedule: ScheduleConfig  # New field
   
    # ... (existing properties remain the same)
    
    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> 'Config':
        if config_path is None:
            root_dir = Path(__file__).parent.parent.parent  # Adjust as needed for your project structure
            config_path = root_dir / 'config' / 'config.yaml'
            
        if not config_path.exists():
            raise FileNotFoundError(f"Config file not found: {config_path}")
            
        try:
            with open(config_path) as f:
                logger.debug(f"Reading config file from: {config_path}")
                data = yaml.safe_load(f)
                logger.debug(f"Loaded YAML data: {data}")
                
            if not isinstance(data, dict):
                raise ValueError(f"Expected dict, got {type(data)}: {data}")

            return cls(
                api=APIConfig(**data['api']),
                database=DatabaseConfig(**data['database']),
                logging=LoggingConfig(**data['logging']),
                sync=SyncConfig(
                    polling_interval=data['sync']['polling_interval'],
                    request_delay=data['sync']['request_delay'],
                    max_retries=data['sync']['max_retries'],
                    delay_between_orders=data['sync']['delay_between_orders'],
                    delay_between_pages=data['sync']['delay_between_pages'],
                    delay_on_error=data['sync']['delay_on_error']
                ),
                schedule=ScheduleConfig(
                    start_hour=data['schedule']['start_hour'],
                    start_minute=data['schedule']['start_minute'],
                    end_hour=data['schedule']['end_hour'],
                    end_minute=data['schedule']['end_minute'],
                    active_days=data['schedule']['active_days']
                )
            )
        except Exception as e:
            logger.error(f"Error loading config: {str(e)}")
            logger.error(f"Config data: {data if 'data' in locals() else 'No data loaded'}")
            raise

# Global config instance
_config: Optional[Config] = None

def get_config(config_path: Optional[Path] = None) -> Config:
    global _config
    try:
        if _config is None:
            logger.debug("Loading configuration...")
            _config = Config.load(config_path)
            logger.debug("Configuration loaded successfully")
        return _config
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise