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
    password: str
    driver: str

    @property
    def connection_string(self) -> str:
        return f"mssql+pyodbc://{self.username}:{self.password}@{self.server}/{self.database}?driver={self.driver}"

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
class RestaurantConfig:
    name: str
    username: str
    password: str

@dataclass
class Config:
    api: APIConfig
    database: DatabaseConfig
    logging: LoggingConfig
    sync: SyncConfig
    restaurants: List[RestaurantConfig]
    
    # Add properties to expose sync configuration at top level
    @property
    def polling_interval(self) -> int:
        return self.sync.polling_interval
    
    @property
    def request_delay(self) -> float:
        return self.sync.request_delay
    
    @property
    def max_retries(self) -> int:
        return self.sync.max_retries
    
    @property
    def log_filename(self) -> str:
        return self.logging.filename
    
    @classmethod
    def load(cls, config_path: Optional[Path] = None) -> 'Config':
        if config_path is None:
            current_dir = Path(__file__).parent.parent  # Move up one directory since settings.py is in config/
            config_path = current_dir / 'config' / 'config.yaml'
            
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
                restaurants=[RestaurantConfig(**r) for r in data['restaurants']]
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
            # Fix: Correct variable assignment
            _config = Config.load(config_path)
            logger.debug("Configuration loaded successfully")
        return _config
    except Exception as e:
        logger.error(f"Failed to load configuration: {str(e)}")
        import traceback
        logger.error(f"Traceback: {traceback.format_exc()}")
        raise