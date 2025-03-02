# File location: src/services/credential_manager.py
from datetime import datetime
import logging
from sqlalchemy import text
from sqlalchemy.orm import Session
from sqlalchemy.exc import NoResultFound
from src.database.models import User
from typing import Optional
import yaml
import os


class CredentialManagerService:
    def __init__(self, session: Session, passphrase: str = None):
        self.session = session
        self.passphrase = passphrase
        self.logger = logging.getLogger(__name__)

    def store_credentials(self, username: str, password: str, company_id: int, restaurant_id: int, company_name: str, active: bool) -> None:
        """
        Store encrypted credentials for a user with company information.
        If the username exists, update the password; otherwise, insert a new record.
        """
        try:
            # Check if the user exists
            self.logger.debug(f"Checking if user {username} exists...")
            user = self.session.query(User).filter_by(username=username).one()
            
            # Update existing user's credentials
            self.session.execute(
                text("""
                    UPDATE restaurant_users 
                    SET 
                        password = ENCRYPTBYPASSPHRASE(:passphrase, :password),
                        company_id = :company_id,
                        restaurant_id = :restaurant_id,
                        company_name = :company_name,
                        active = :active,
                        last_updated = :last_updated
                    WHERE username = :username
                """),
                {
                    'passphrase': self.passphrase,
                    'password': password,
                    'company_id': company_id,
                    'restaurant_id': restaurant_id,
                    'company_name': company_name,
                    'active': active,
                    'last_updated': datetime.now(),
                    'username': username
                }
            )
        except NoResultFound:
            # Insert a new user
            self.logger.debug(f"User not found. Inserting new user {username}...")
            self.session.execute(
                text("""
                    INSERT INTO restaurant_users 
                    (username, password, company_id, restaurant_id, company_name, active, created_at, last_updated) 
                    VALUES 
                    (:username, ENCRYPTBYPASSPHRASE(:passphrase, :password), :company_id, :restaurant_id, :company_name, :active, :created_at, :last_updated)
                """),
                {
                    'username': username,
                    'password': password,
                    'passphrase': self.passphrase,
                    'company_id': company_id,
                    'restaurant_id': restaurant_id,
                    'company_name': company_name,
                    'active': active,
                    'created_at': datetime.now(),
                    'last_updated': datetime.now()
                }
            )

        # Commit the transaction
        self.session.commit()

    def import_credentials_from_yaml(self, yaml_path: str = "creds/creds.yaml") -> Optional[dict]:
        """
        Import credentials from a YAML file into the database and then remove the credentials
        from the YAML file for security.
        
        Args:
            yaml_path (str): Path to the YAML file containing credentials. Defaults to "creds.yaml"
            
        Returns:
            Optional[dict]: Summary of imported credentials or None if file doesn't exist
            
        Raises:
            yaml.YAMLError: If YAML file is malformed
            ValueError: If YAML file is missing required fields or has invalid values
        """
        if not os.path.exists(yaml_path):
            return None
            
        try:
            self.logger.debug(f"Importing credentials from {yaml_path}")
            # Read the YAML file
            with open(yaml_path, 'r') as file:
                creds_data = yaml.safe_load(file)
                
            if not isinstance(creds_data, list):
                creds_data = [creds_data]
                
            # Track import results
            results = {
                'imported': 0,
                'failed': 0,
                'usernames': []
            }
                
            # Process each credential entry
            for cred in creds_data:
                try:
                    # Validate required fields exist and have valid values
                    required_fields = {
                        'username': str,
                        'password': str,
                        'company_id': int,
                        'restaurant_id': int,
                        'company_name': str,
                        'active': bool
                    }

                    # Check for missing or empty fields
                    missing_fields = []
                    invalid_values = []
                    
                    for field, expected_type in required_fields.items():
                        # Check if field exists
                        if field not in cred:
                            missing_fields.append(field)
                            continue
                            
                        # Check if field has a value (not None and not empty string for str)
                        if cred[field] is None:
                            invalid_values.append(f"{field} cannot be None")
                        elif expected_type == str and not str(cred[field]).strip():
                            invalid_values.append(f"{field} cannot be empty")
                        elif not isinstance(cred[field], expected_type):
                            invalid_values.append(f"{field} must be of type {expected_type.__name__}")

                    if missing_fields:
                        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
                    if invalid_values:
                        raise ValueError(f"Invalid values: {', '.join(invalid_values)}")
                    
                    # Store credentials in database
                    self.store_credentials(
                        username=cred['username'],
                        password=cred['password'],
                        company_id=cred['company_id'],
                        restaurant_id=cred['restaurant_id'],
                        company_name=cred['company_name'],
                        active=cred['active']
                    )
                    
                    results['imported'] += 1
                    results['usernames'].append(cred['username'])
                    
                except (ValueError, Exception) as e:
                    self.logger.error(f"Failed to import credential: {str(e)}")
                    results['failed'] += 1
                    continue
                    
            # Securely remove credentials from file
            try:
                # First overwrite with empty data
                with open(yaml_path, 'w') as file:
                    file.write('')
                # Then delete the file
                os.remove(yaml_path)
            except OSError as e:
                # Log error but don't raise since credentials were imported
                self.logger.warning(f"Could not remove credentials file: {e}")
                
            return results
            
        except yaml.YAMLError as e:
            raise yaml.YAMLError(f"Error parsing YAML file: {e}")
        except Exception as e:
            raise Exception(f"Error importing credentials: {e}")
    


    def get_credential_by_restaurant(self, restaurant_id: int) -> Optional[dict]:
        """Retrieve credential for a specific restaurant."""
        query = text("""
            SELECT TOP 1
                username,
                CONVERT(NVARCHAR, DECRYPTBYPASSPHRASE(:passphrase, password)) as password,
                company_id,
                restaurant_id,
                company_name,
                created_at,
                last_updated
            FROM restaurant_users  -- Corrected table name
            WHERE restaurant_id = :restaurant_id
        """)
        
        # Execute the query with the correct parameters
        result = self.session.execute(query, {
            'passphrase': self.passphrase,
            'restaurant_id': restaurant_id  # Ensure this matches the parameter in the query
        }).first()

        # Handle the result
        if not result:
            return None

        return {
            'username': result.username,
            'password': result.password,
            'company_id': result.company_id,
            'restaurant_id': result.restaurant_id,
            'company_name': result.company_name,
            'created_at': result.created_at,
            'last_updated': result.last_updated
        }

    def list_credentials(self) -> list[User]:
        """List all active users with detailed information, including decrypted passwords."""
        query = text("""
            SELECT
                username,
                CONVERT(NVARCHAR, DECRYPTBYPASSPHRASE(:passphrase, password)) AS password,
                company_id,
                restaurant_id,
                company_name,
                created_at,
                last_updated,
                active
            FROM restaurant_users
            WHERE active = 1
        """)

        # Execute the query
        results = self.session.query(User).filter_by(active=True).all()

        return results