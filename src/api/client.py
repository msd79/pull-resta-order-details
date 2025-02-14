# File location: src/api/client.py
import aiohttp
import json
import logging
import base64

class RestaAPI:
    def __init__(self, base_url, page_size=5):
        self.base_url = base_url
        self.page_size = page_size
        self.session_token = None
        self.company_id = None
        self.restaurant_id = None
        self.company_name = None
        self.restaurant_name = None
        self.logger = logging.getLogger(__name__)
        self._session = None

    async def __aenter__(self):
        self._session = aiohttp.ClientSession()
        return self

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self._session:
            await self._session.close()

    @staticmethod
    def decode_jwt_payload(self, token):
        """Decode JWT token payload without verification"""
        try:
            parts = token.split('.')
            if len(parts) != 3:
                raise ValueError("Invalid token format")
                
            padding = '=' * (4 - len(parts[1]) % 4)
            payload = base64.b64decode(parts[1] + padding)
            return json.loads(payload)
        except Exception as e:
            self.logger.error(f"Error decoding token: {e}")
            return None
    
    async def login(self, email, password):
        """Handle API login and token management"""
        login_url = f"{self.base_url}/Account/Login"
        params = {
            "email": email,
            "password": password,
            "apiClient": 1,
            "apiClientVersion": 196
        }
        headers = {
            "User-Agent": "RestSharp/105.2.3.0",
            "Accept-Encoding": "gzip, deflate",
            "Accept": "application/json, application/xml, text/json, text/x-json, text/javascript, text/xml"
        }
        
        if not self._session:
            self._session = aiohttp.ClientSession()
        
        async with self._session.post(login_url, params=params, headers=headers) as response:
            if response.status != 200:
                raise Exception(f"Login failed with status code: {response.status}")
            
            try:
                data = await response.json()
                self.logger.debug("Login response:", json.dumps(data, indent=2))
              

                
                self.session_token = data.get('SessionToken')
                if not self.session_token:
                    self.logger.error("Session token not found in response:", data)
                    raise Exception("Session token not found in response")
                
                # First try to get company_id from token
                token_payload = self.decode_jwt_payload(self, self.session_token)
                if token_payload and 'CompanyID' in token_payload:
                    self.company_id = int(token_payload['CompanyID'])
                else:
                    # Fallback to getting company_id from response
                    self.company_id = data.get('Company', {}).get('ID')

                    if not self.company_id:
                        raise Exception("Could not find company ID in response or token")
                self.restaurant_id = data.get('Restaurant', {}).get('ID')
                self.restaurant_name = data.get('Restaurant', {}).get('Name')
                self.company_name = data.get('Company', {}).get('Name')
                
                
                return self.session_token, self.company_id
                
            except json.JSONDecodeError as e:
                text = await response.text()
                
                self.logger.debug("Raw response:", text)
                raise Exception(f"Failed to parse login response: {e}")

    async def get_orders_list(self, page_index):
        """Fetch orders list with pagination"""
        if not self.session_token:
            raise Exception("Not logged in. Call login() first.")
        
        return await self._make_request(
            "GET",
            f"{self.base_url}/Order/List",
            params={
                "pageSize": self.page_size,
                "pageIndex": page_index,
                "sessionToken": self.session_token
            }
        )

    async def fetch_order_details(self, order_id):
        """Fetch detailed information for a specific order"""
        self.logger.info(f"Fetching order details for ID: {order_id}")
        return await self._make_request(
            "GET",
            f"{self.base_url}/order/Detail",
            params={
                "ID": order_id,
                "SessionToken": self.session_token
            }
        )

    async def _make_request(self, method, url, **kwargs):
        """Centralized request handling with error management"""
        if not self._session:
            self._session = aiohttp.ClientSession()
            
        try:
            async with self._session.request(method, url, **kwargs) as response:
                response.raise_for_status()
                return await response.json()
        except aiohttp.ClientError as e:
            self.logger.error(f"API request failed: {str(e)}")
            raise

    async def close(self):
        """Close the aiohttp session"""
        if self._session:
            await self._session.close()
            self._session = None