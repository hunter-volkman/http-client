import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Sequence, Tuple

import requests
from typing_extensions import Self
from viam.components.sensor import Sensor
from viam.logging import getLogger
from viam.proto.app.robot import ComponentConfig
from viam.proto.common import Geometry, ResourceName
from viam.resource.base import ResourceBase
from viam.resource.easy_resource import EasyResource
from viam.resource.types import Model, ModelFamily
from viam.utils import SensorReading, ValueTypes, struct_to_dict

LOGGER = getLogger(__name__)


class Mission(Sensor, EasyResource):
    """
    YOOBIC Mission Management for Pret Monitoring
    
    Handles mission lifecycle for temperature monitoring and inventory tracking.
    Integrates Viam sensor data with YOOBIC operational workflows.
    """

    MODEL: ClassVar[Model] = Model(ModelFamily("hunter", "yoobic"), "mission")

    def __init__(self, name: str):
        super().__init__(name)
        
        # YOOBIC Configuration
        self._yoobic_username = None
        self._yoobic_password = None
        self._environment = None
        self._base_url = None
        
        # Authentication State
        self._jwt_token = None
        self._token_expires_at = None
        
        # Mission Configuration
        self._store_locations = []
        self._sync_interval = 300  # 5 minutes
        self._temperature_threshold = 4.0  # Celsius
        self._auto_create_missions = True
        
        # Status & Metrics
        self._configured = False
        self._last_sync = None
        self._sync_errors = []
        self._mission_stats = {
            "missions_created": 0,
            "missions_completed": 0,
            "alerts_sent": 0,
            "sync_count": 0
        }
        
        # Background Tasks
        self._sync_task = None
        self._session = None

    @classmethod
    def new(
        cls, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ) -> Self:
        instance = cls(config.name)
        instance.reconfigure(config, dependencies)
        return instance

    @classmethod
    def validate_config(
        cls, config: ComponentConfig
    ) -> Tuple[Sequence[str], Sequence[str]]:
        """Validate configuration and return dependencies."""
        attrs = struct_to_dict(config.attributes) if config.attributes else {}
        
        # Required fields
        required = ["yoobic_username", "yoobic_password", "environment"]
        for field in required:
            if not attrs.get(field):
                raise ValueError(f"{field} is required")
        
        # Validate environment
        if attrs.get("environment") not in ["sandbox", "production"]:
            raise ValueError("environment must be 'sandbox' or 'production'")
            
        # Validate store locations
        store_locations = attrs.get("store_locations", [])
        if not isinstance(store_locations, list):
            raise ValueError("store_locations must be a list")
        
        return [], []  # No dependencies

    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """Reconfigure the mission sensor."""
        attrs = struct_to_dict(config.attributes) if config.attributes else {}
        
        # Stop existing background task
        self._stop_sync_task()
        
        # Update configuration
        self._yoobic_username = attrs["yoobic_username"]
        self._yoobic_password = attrs["yoobic_password"] 
        self._environment = attrs["environment"]
        
        # Set base URL
        if self._environment == "production":
            self._base_url = "https://api.yoobic.com/public/api"
        else:
            self._base_url = "https://api-sandbox.yoobic.com/public/api"
        
        # Optional configuration
        self._store_locations = attrs.get("store_locations", [])
        self._sync_interval = attrs.get("sync_interval", 300)
        self._temperature_threshold = attrs.get("temperature_threshold", 4.0)
        self._auto_create_missions = attrs.get("auto_create_missions", True)
        
        # Initialize HTTP session
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": f"Viam-YOOBIC-Mission/{self.name}",
            "Content-Type": "application/json"
        })
        
        # Reset authentication
        self._jwt_token = None
        self._token_expires_at = None
        
        self._configured = True
        
        LOGGER.info(f"Mission sensor configured: {self._environment}")
        LOGGER.info(f"Store locations: {len(self._store_locations)}")
        LOGGER.info(f"Sync interval: {self._sync_interval}s")
        
        # Start background sync
        if self._configured and self._auto_create_missions:
            self._start_sync_task()

    async def get_readings(
        self,
        *,
        extra: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Mapping[str, SensorReading]:
        """Get mission sensor status and metrics."""
        if not self._configured:
            return {"error": "Module not configured", "configured": False}
        
        try:
            # Force sync if requested
            if extra and extra.get("force_sync"):
                await self._perform_sync()
            
            # Calculate next sync time
            next_sync_in = 0
            if self._last_sync:
                elapsed = (datetime.now() - self._last_sync).total_seconds()
                next_sync_in = max(0, self._sync_interval - elapsed)
            
            readings = {
                # Configuration
                "configured": self._configured,
                "environment": self._environment,
                "store_count": len(self._store_locations),
                "auto_create_enabled": self._auto_create_missions,
                
                # Status
                "last_sync": self._last_sync.isoformat() if self._last_sync else None,
                "next_sync_in": round(next_sync_in, 1),
                "auth_valid": self._is_token_valid(),
                "sync_errors": len(self._sync_errors),
                
                # Metrics
                "mission_stats": self._mission_stats,
                "temperature_threshold": self._temperature_threshold,
                "background_task_running": self._sync_task is not None and not self._sync_task.done()
            }
            
            return readings
            
        except Exception as e:
            LOGGER.error(f"Error getting readings: {e}")
            return {"error": str(e)}

    async def do_command(
        self,
        command: Mapping[str, ValueTypes],
        *,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Mapping[str, ValueTypes]:
        """Handle mission commands."""
        cmd = command.get("command", "")
        
        if cmd == "sync_now":
            return await self._manual_sync()
        elif cmd == "create_mission":
            return await self._create_mission_command(command)
        elif cmd == "check_auth":
            return await self._check_auth()
        elif cmd == "get_missions":
            return await self._get_missions_command(command)
        elif cmd == "validate_mission":
            return await self._validate_mission_command(command)
        elif cmd == "test_connection":
            return await self._test_connection()
        elif cmd == "get_stats":
            return {"stats": self._mission_stats, "errors": self._sync_errors[-5:]}
        elif cmd == "clear_errors":
            self._sync_errors.clear()
            return {"success": True, "message": "Errors cleared"}
        else:
            return {
                "error": f"Unknown command: {cmd}",
                "available_commands": [
                    "sync_now", "create_mission", "check_auth", "get_missions",
                    "validate_mission", "test_connection", "get_stats", "clear_errors"
                ]
            }

    # Authentication Methods
    def _is_token_valid(self) -> bool:
        """Check if JWT token is valid."""
        return (self._jwt_token is not None and 
                self._token_expires_at is not None and 
                datetime.now() < self._token_expires_at)

    async def _authenticate(self) -> bool:
        """Authenticate with YOOBIC API."""
        if self._is_token_valid():
            return True
            
        try:
            auth_data = {
                "username": self._yoobic_username,
                "password": self._yoobic_password
            }
            
            LOGGER.info(f"Authenticating with YOOBIC API ({self._environment})")
            response = self._session.post(f"{self._base_url}/auth/login", json=auth_data, timeout=30)
            
            if response.status_code == 200:
                data = response.json()
                self._jwt_token = data.get("token")
                
                # Set token expiry (50 minutes for safety)
                self._token_expires_at = datetime.now() + timedelta(minutes=50)
                
                # Update session headers
                self._session.headers["Authorization"] = f"Bearer {self._jwt_token}"
                
                LOGGER.info("Successfully authenticated with YOOBIC")
                return True
            else:
                LOGGER.error(f"Authentication failed: {response.status_code} - {response.text}")
                return False
                
        except Exception as e:
            LOGGER.error(f"Authentication error: {e}")
            return False

    # Mission Operations
    async def _create_temperature_mission(self, store_id: str, temperature: float) -> Dict:
        """Create a temperature monitoring mission."""
        try:
            if not await self._authenticate():
                raise Exception("Authentication failed")
            
            # Determine mission priority based on temperature
            if temperature > self._temperature_threshold + 2:
                priority = "high"
                status = "critical"
            elif temperature > self._temperature_threshold:
                priority = "medium" 
                status = "warning"
            else:
                priority = "low"
                status = "normal"
            
            mission_data = {
                "title": f"Temperature Check - {store_id}",
                "type": "mission",
                "store_id": store_id,
                "due_date": (datetime.now() + timedelta(hours=1)).isoformat(),
                "custom_fields": {
                    "temperature": temperature,
                    "threshold": self._temperature_threshold,
                    "status": status,
                    "priority": priority,
                    "sensor_type": "temperature",
                    "created_by": "viam_mission_module"
                }
            }
            
            response = self._session.post(f"{self._base_url}/missions", json=mission_data, timeout=30)
            
            if response.status_code in [200, 201]:
                mission = response.json()
                self._mission_stats["missions_created"] += 1
                
                LOGGER.info(f"Created temperature mission for {store_id}: {temperature}°C")
                return {"success": True, "mission_id": mission.get("mission_id"), "status": status}
            else:
                raise Exception(f"Mission creation failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            LOGGER.error(f"Error creating temperature mission: {e}")
            return {"success": False, "error": str(e)}

    # Background Sync
    def _start_sync_task(self):
        """Start background sync task."""
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
        
        self._sync_task = asyncio.create_task(self._sync_loop())
        LOGGER.info(f"Started background sync task (interval: {self._sync_interval}s)")

    def _stop_sync_task(self):
        """Stop background sync task."""
        if self._sync_task and not self._sync_task.done():
            self._sync_task.cancel()
        self._sync_task = None

    async def _sync_loop(self):
        """Background sync loop."""
        while True:
            try:
                await asyncio.sleep(self._sync_interval)
                await self._perform_sync()
            except asyncio.CancelledError:
                LOGGER.info("Sync task cancelled")
                break
            except Exception as e:
                LOGGER.error(f"Error in sync loop: {e}")

    async def _perform_sync(self):
        """Perform data synchronization with YOOBIC."""
        try:
            LOGGER.info("Starting sync operation")
            self._mission_stats["sync_count"] += 1
            
            # Get mock temperature data (replace with real Viam data)
            temperature_data = await self._get_temperature_data()
            
            # Create missions for temperature violations
            missions_created = 0
            for reading in temperature_data:
                if reading["temperature"] > self._temperature_threshold:
                    result = await self._create_temperature_mission(
                        reading["store_id"], 
                        reading["temperature"]
                    )
                    if result["success"]:
                        missions_created += 1
            
            self._last_sync = datetime.now()
            LOGGER.info(f"Sync completed: {missions_created} missions created")
            
        except Exception as e:
            error_info = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "type": "sync_error"
            }
            self._sync_errors.append(error_info)
            self._sync_errors = self._sync_errors[-10:]  # Keep last 10 errors
            LOGGER.error(f"Sync failed: {e}")

    async def _get_temperature_data(self) -> List[Dict]:
        """Get temperature data (mock implementation)."""
        # TODO: Replace with real Viam data client integration
        import random
        
        mock_data = []
        for store_id in self._store_locations or ["store_001", "store_002", "store_003"]:
            # Generate realistic temperature with some above threshold
            base_temp = 2.5
            variation = random.uniform(-1.5, 4.0)  # Some will exceed threshold
            temperature = round(base_temp + variation, 1)
            
            mock_data.append({
                "store_id": store_id,
                "temperature": temperature,
                "timestamp": datetime.now().isoformat(),
                "sensor_id": f"temp_sensor_{store_id}"
            })
        
        return mock_data

    # DoCommand Implementations
    async def _manual_sync(self) -> Dict[str, Any]:
        """Manually trigger sync."""
        try:
            await self._perform_sync()
            return {
                "success": True,
                "message": "Sync completed",
                "last_sync": self._last_sync.isoformat() if self._last_sync else None
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _check_auth(self) -> Dict[str, Any]:
        """Check authentication status."""
        try:
            auth_success = await self._authenticate()
            return {
                "authenticated": auth_success,
                "token_valid": self._is_token_valid(),
                "environment": self._environment,
                "base_url": self._base_url
            }
        except Exception as e:
            return {"authenticated": False, "error": str(e)}

    async def _test_connection(self) -> Dict[str, Any]:
        """Test YOOBIC API connection."""
        try:
            if not await self._authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            # Test API call
            response = self._session.get(f"{self._base_url}/tenants", timeout=10)
            
            return {
                "success": response.status_code == 200,
                "status_code": response.status_code,
                "response_time": response.elapsed.total_seconds(),
                "environment": self._environment
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _create_mission_command(self, command: Dict) -> Dict[str, Any]:
        """Create mission via command."""
        try:
            store_id = command.get("store_id")
            temperature = command.get("temperature")
            
            if not store_id or temperature is None:
                return {"success": False, "error": "store_id and temperature required"}
            
            result = await self._create_temperature_mission(store_id, float(temperature))
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _get_missions_command(self, command: Dict) -> Dict[str, Any]:
        """Get missions via command."""
        try:
            if not await self._authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            store_id = command.get("store_id")
            limit = command.get("limit", 10)
            
            # Build filter
            filter_params = {"limit": limit}
            if store_id:
                filter_params["where"] = {"store_id": store_id}
            
            response = self._session.get(
                f"{self._base_url}/missions",
                params={"filter": json.dumps(filter_params)},
                timeout=30
            )
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "missions": data.get("data", []),
                    "count": len(data.get("data", []))
                }
            else:
                return {"success": False, "error": f"API error: {response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _validate_mission_command(self, command: Dict) -> Dict[str, Any]:
        """Validate mission via command."""
        try:
            if not await self._authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            mission_id = command.get("mission_id")
            compliant = command.get("compliant", True)
            
            if not mission_id:
                return {"success": False, "error": "mission_id required"}
            
            validation_data = {
                "compliant": compliant,
                "rating": 5 if compliant else 2,
                "reason_noncompliant": None if compliant else "Temperature threshold exceeded"
            }
            
            response = self._session.post(
                f"{self._base_url}/missions/{mission_id}/validate",
                json=validation_data,
                timeout=30
            )
            
            if response.status_code in [200, 201]:
                self._mission_stats["missions_completed"] += 1
                return {"success": True, "validated": compliant}
            else:
                return {"success": False, "error": f"Validation failed: {response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def close(self):
        """Clean up resources."""
        LOGGER.info(f"Shutting down mission sensor: {self.name}")
        self._stop_sync_task()
        
        if self._session:
            self._session.close()
            
        await super().close()

    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> List[Geometry]:
        """Return empty geometries (this is a data integration component)."""
        return []