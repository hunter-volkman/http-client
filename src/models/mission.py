import asyncio
import json
import time
from datetime import datetime, timedelta
from typing import Any, ClassVar, Dict, List, Mapping, Optional, Sequence, Tuple, Union

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


class MissionManager(Sensor, EasyResource):
    """
    Universal Mission Management Module
    
    Integrates with operational workflow platforms to automate data collection,
    mission creation, and compliance monitoring. Designed to be brand-agnostic
    and configurable for various retail and operational use cases.
    """

    MODEL: ClassVar[Model] = Model(ModelFamily("hunter", "yoobic"), "mission")

    def __init__(self, name: str):
        super().__init__(name)
        
        # Platform Configuration
        self._platform_config = {}
        self._authentication = {}
        
        # Mission Configuration
        self._mission_rules = []
        self._data_sources = []
        self._notification_config = {}
        
        # Operational Settings
        self._sync_interval = 300  # 5 minutes default
        self._batch_size = 50
        self._retry_attempts = 3
        self._timeout = 30
        
        # State Management
        self._configured = False
        self._last_sync = None
        self._sync_errors = []
        self._mission_stats = {
            "missions_created": 0,
            "missions_completed": 0,
            "alerts_sent": 0,
            "sync_count": 0,
            "error_count": 0,
            "uptime_start": datetime.now()
        }
        
        # Runtime
        self._sync_task = None
        self._session = None
        self._auth_token = None
        self._token_expires_at = None

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
        
        # Validate platform configuration
        platform_config = attrs.get("platform", {})
        if not platform_config:
            raise ValueError("platform configuration is required")
        
        # Check required platform fields
        required_platform_fields = ["type", "base_url", "authentication"]
        for field in required_platform_fields:
            if field not in platform_config:
                raise ValueError(f"platform.{field} is required")
        
        # Validate authentication
        auth_config = platform_config.get("authentication", {})
        auth_method = auth_config.get("method", "")
        if auth_method not in ["username_password", "api_key", "oauth"]:
            raise ValueError("authentication.method must be 'username_password', 'api_key', or 'oauth'")
        
        # Validate mission rules
        mission_rules = attrs.get("mission_rules", [])
        if not isinstance(mission_rules, list):
            raise ValueError("mission_rules must be a list")
        
        for i, rule in enumerate(mission_rules):
            if not isinstance(rule, dict):
                raise ValueError(f"mission_rules[{i}] must be an object")
            
            required_rule_fields = ["name", "trigger", "action"]
            for field in required_rule_fields:
                if field not in rule:
                    raise ValueError(f"mission_rules[{i}].{field} is required")
        
        return [], []  # No dependencies for now

    def reconfigure(
        self, config: ComponentConfig, dependencies: Mapping[ResourceName, ResourceBase]
    ):
        """Reconfigure the mission manager."""
        attrs = struct_to_dict(config.attributes) if config.attributes else {}
        
        # Stop existing tasks
        self._stop_sync_task()
        
        # Load platform configuration
        self._platform_config = attrs.get("platform", {})
        
        # Load mission rules
        self._mission_rules = attrs.get("mission_rules", [])
        
        # Load data sources
        self._data_sources = attrs.get("data_sources", [])
        
        # Load notification config
        self._notification_config = attrs.get("notifications", {})
        
        # Load operational settings
        self._sync_interval = attrs.get("sync_interval", 300)
        self._batch_size = attrs.get("batch_size", 50)
        self._retry_attempts = attrs.get("retry_attempts", 3)
        self._timeout = attrs.get("timeout", 30)
        
        # Initialize HTTP session
        self._session = requests.Session()
        self._session.headers.update({
            "User-Agent": f"Viam-Mission-Manager/{self.name}",
            "Content-Type": "application/json"
        })
        
        # Reset authentication
        self._auth_token = None
        self._token_expires_at = None
        
        self._configured = True
        
        LOGGER.info(f"Mission Manager configured")
        LOGGER.info(f"Platform: {self._platform_config.get('type', 'unknown')}")
        LOGGER.info(f"Base URL: {self._platform_config.get('base_url', 'unknown')}")
        LOGGER.info(f"Mission Rules: {len(self._mission_rules)}")
        LOGGER.info(f"Data Sources: {len(self._data_sources)}")
        LOGGER.info(f"Sync Interval: {self._sync_interval}s")
        
        # Start background sync if auto-sync is enabled
        if attrs.get("auto_sync", True):
            self._start_sync_task()

    async def get_readings(
        self,
        *,
        extra: Optional[Mapping[str, Any]] = None,
        timeout: Optional[float] = None,
        **kwargs
    ) -> Mapping[str, SensorReading]:
        """Get mission manager status and metrics."""
        if not self._configured:
            return {"error": "Mission manager not configured", "configured": False}
        
        try:
            # Force sync if requested
            if extra and extra.get("force_sync"):
                await self._perform_sync()
            
            # Calculate uptime
            uptime = (datetime.now() - self._mission_stats["uptime_start"]).total_seconds()
            
            # Calculate next sync time
            next_sync_in = 0
            if self._last_sync:
                elapsed = (datetime.now() - self._last_sync).total_seconds()
                next_sync_in = max(0, self._sync_interval - elapsed)
            
            readings = {
                # Configuration
                "configured": self._configured,
                "platform_type": self._platform_config.get("type", "unknown"),
                "base_url": self._platform_config.get("base_url", "unknown"),
                "mission_rules_count": len(self._mission_rules),
                "data_sources_count": len(self._data_sources),
                
                # Status
                "last_sync": self._last_sync.isoformat() if self._last_sync else None,
                "next_sync_in": round(next_sync_in, 1),
                "auth_valid": self._is_token_valid(),
                "sync_errors": len(self._sync_errors),
                "uptime_seconds": round(uptime, 1),
                
                # Metrics
                "mission_stats": self._mission_stats,
                "background_task_running": self._sync_task is not None and not self._sync_task.done(),
                
                # Operational
                "sync_interval": self._sync_interval,
                "batch_size": self._batch_size,
                "retry_attempts": self._retry_attempts
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
            return await self._get_stats()
        elif cmd == "clear_errors":
            return await self._clear_errors()
        elif cmd == "evaluate_rules":
            return await self._evaluate_rules_command(command)
        else:
            return {
                "error": f"Unknown command: {cmd}",
                "available_commands": [
                    "sync_now", "create_mission", "check_auth", "get_missions",
                    "validate_mission", "test_connection", "get_stats", "clear_errors",
                    "evaluate_rules"
                ]
            }

    # Authentication Methods
    def _is_token_valid(self) -> bool:
        """Check if authentication token is valid."""
        return (self._auth_token is not None and 
                self._token_expires_at is not None and 
                datetime.now() < self._token_expires_at)

    async def _authenticate(self) -> bool:
        """Authenticate with the platform API."""
        if self._is_token_valid():
            return True
        
        try:
            auth_config = self._platform_config.get("authentication", {})
            method = auth_config.get("method", "")
            
            if method == "username_password":
                return await self._authenticate_username_password(auth_config)
            elif method == "api_key":
                return await self._authenticate_api_key(auth_config)
            elif method == "oauth":
                return await self._authenticate_oauth(auth_config)
            else:
                LOGGER.error(f"Unsupported authentication method: {method}")
                return False
                
        except Exception as e:
            LOGGER.error(f"Authentication error: {e}")
            return False

    async def _authenticate_username_password(self, auth_config: Dict) -> bool:
        """Authenticate using username/password."""
        username = auth_config.get("username")
        password = auth_config.get("password")
        
        if not username or not password:
            LOGGER.error("Username and password required for authentication")
            return False
        
        auth_endpoint = auth_config.get("endpoint", "/auth/login")
        url = f"{self._platform_config['base_url']}{auth_endpoint}"
        
        auth_data = {
            "username": username,
            "password": password
        }
        
        LOGGER.info(f"Authenticating with platform at {url}")
        response = self._session.post(url, json=auth_data, timeout=self._timeout)
        
        if response.status_code == 200:
            data = response.json()
            self._auth_token = data.get("token")
            
            # Set token expiry (default 50 minutes for safety)
            expires_in = auth_config.get("token_expires_minutes", 50)
            self._token_expires_at = datetime.now() + timedelta(minutes=expires_in)
            
            # Update session headers
            self._session.headers["Authorization"] = f"Bearer {self._auth_token}"
            
            LOGGER.info("Successfully authenticated with platform")
            return True
        else:
            LOGGER.error(f"Authentication failed: {response.status_code} - {response.text}")
            return False

    async def _authenticate_api_key(self, auth_config: Dict) -> bool:
        """Authenticate using API key."""
        api_key = auth_config.get("api_key")
        if not api_key:
            LOGGER.error("API key required for authentication")
            return False
        
        # Set API key in headers
        header_name = auth_config.get("header_name", "X-API-Key")
        self._session.headers[header_name] = api_key
        
        # API keys typically don't expire, but set a long expiry
        self._token_expires_at = datetime.now() + timedelta(days=365)
        
        LOGGER.info("API key authentication configured")
        return True

    async def _authenticate_oauth(self, auth_config: Dict) -> bool:
        """Authenticate using OAuth (placeholder for future implementation)."""
        LOGGER.error("OAuth authentication not yet implemented")
        return False

    # Mission Rule Engine
    async def _evaluate_mission_rules(self, data: Dict) -> List[Dict]:
        """Evaluate mission rules against provided data."""
        missions_to_create = []
        
        for rule in self._mission_rules:
            try:
                if await self._evaluate_rule(rule, data):
                    mission = await self._create_mission_from_rule(rule, data)
                    if mission:
                        missions_to_create.append(mission)
            except Exception as e:
                LOGGER.error(f"Error evaluating rule {rule.get('name', 'unknown')}: {e}")
        
        return missions_to_create

    async def _evaluate_rule(self, rule: Dict, data: Dict) -> bool:
        """Evaluate a single mission rule."""
        trigger = rule.get("trigger", {})
        trigger_type = trigger.get("type", "")
        
        if trigger_type == "threshold":
            return self._evaluate_threshold_trigger(trigger, data)
        elif trigger_type == "schedule":
            return self._evaluate_schedule_trigger(trigger, data)
        elif trigger_type == "event":
            return self._evaluate_event_trigger(trigger, data)
        else:
            LOGGER.warning(f"Unknown trigger type: {trigger_type}")
            return False

    def _evaluate_threshold_trigger(self, trigger: Dict, data: Dict) -> bool:
        """Evaluate threshold-based trigger."""
        field = trigger.get("field", "")
        operator = trigger.get("operator", "")
        threshold = trigger.get("threshold", 0)
        
        if not field or field not in data:
            return False
        
        value = data[field]
        
        if operator == "gt":
            return value > threshold
        elif operator == "lt":
            return value < threshold
        elif operator == "gte":
            return value >= threshold
        elif operator == "lte":
            return value <= threshold
        elif operator == "eq":
            return value == threshold
        elif operator == "ne":
            return value != threshold
        else:
            LOGGER.warning(f"Unknown operator: {operator}")
            return False

    def _evaluate_schedule_trigger(self, trigger: Dict, data: Dict) -> bool:
        """Evaluate schedule-based trigger."""
        # Implementation for scheduled missions
        return False  # Placeholder

    def _evaluate_event_trigger(self, trigger: Dict, data: Dict) -> bool:
        """Evaluate event-based trigger."""
        # Implementation for event-based missions
        return False  # Placeholder

    async def _create_mission_from_rule(self, rule: Dict, data: Dict) -> Optional[Dict]:
        """Create a mission based on rule and data."""
        try:
            action = rule.get("action", {})
            
            mission_data = {
                "title": action.get("title", "").format(**data),
                "type": action.get("type", "generic"),
                "store_id": data.get("store_id", "unknown"),
                "due_date": (datetime.now() + timedelta(hours=action.get("due_hours", 24))).isoformat(),
                "priority": action.get("priority", "medium"),
                "custom_fields": {
                    "rule_name": rule.get("name"),
                    "triggered_by": data,
                    "created_by": "viam_mission_manager"
                }
            }
            
            # Add any additional fields from the rule
            additional_fields = action.get("custom_fields", {})
            mission_data["custom_fields"].update(additional_fields)
            
            return mission_data
            
        except Exception as e:
            LOGGER.error(f"Error creating mission from rule: {e}")
            return None

    # Data Collection
    async def _collect_data(self) -> List[Dict]:
        """Collect data from configured sources."""
        all_data = []
        
        for source in self._data_sources:
            try:
                data = await self._collect_from_source(source)
                if data:
                    all_data.extend(data)
            except Exception as e:
                LOGGER.error(f"Error collecting from source {source.get('name', 'unknown')}: {e}")
        
        return all_data

    async def _collect_from_source(self, source: Dict) -> List[Dict]:
        """Collect data from a single source."""
        source_type = source.get("type", "")
        
        if source_type == "viam_sensor":
            return await self._collect_viam_sensor_data(source)
        elif source_type == "http_api":
            return await self._collect_http_api_data(source)
        elif source_type == "mock":
            return await self._collect_mock_data(source)
        else:
            LOGGER.warning(f"Unknown data source type: {source_type}")
            return []

    async def _collect_viam_sensor_data(self, source: Dict) -> List[Dict]:
        """Collect data from Viam sensors."""
        # Implementation for collecting from Viam sensors
        return []  # Placeholder

    async def _collect_http_api_data(self, source: Dict) -> List[Dict]:
        """Collect data from HTTP API."""
        # Implementation for collecting from HTTP APIs
        return []  # Placeholder

    async def _collect_mock_data(self, source: Dict) -> List[Dict]:
        """Generate mock data for testing."""
        import random
        
        mock_data = []
        stores = source.get("stores", ["store_001", "store_002"])
        
        for store_id in stores:
            # Generate mock temperature data
            base_temp = source.get("base_temperature", 2.5)
            variation = random.uniform(-2.0, 5.0)
            temperature = round(base_temp + variation, 1)
            
            mock_data.append({
                "store_id": store_id,
                "temperature": temperature,
                "timestamp": datetime.now().isoformat(),
                "sensor_id": f"temp_sensor_{store_id}",
                "unit_name": f"Walk in Fridge"
            })
        
        return mock_data

    # Platform Integration
    async def _create_platform_mission(self, mission_data: Dict) -> Dict:
        """Create a mission on the platform."""
        try:
            if not await self._authenticate():
                raise Exception("Authentication failed")
            
            endpoint = self._platform_config.get("endpoints", {}).get("create_mission", "/missions")
            url = f"{self._platform_config['base_url']}{endpoint}"
            
            response = self._session.post(url, json=mission_data, timeout=self._timeout)
            
            if response.status_code in [200, 201]:
                mission = response.json()
                self._mission_stats["missions_created"] += 1
                
                LOGGER.info(f"Created mission: {mission_data['title']} for {mission_data['store_id']}")
                return {"success": True, "mission": mission}
            else:
                raise Exception(f"Mission creation failed: {response.status_code} - {response.text}")
                
        except Exception as e:
            LOGGER.error(f"Error creating platform mission: {e}")
            self._mission_stats["error_count"] += 1
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
                self._mission_stats["error_count"] += 1

    async def _perform_sync(self):
        """Perform data synchronization and mission creation."""
        try:
            LOGGER.info("Starting sync operation")
            self._mission_stats["sync_count"] += 1
            
            # Collect data from all sources
            data_points = await self._collect_data()
            
            # Evaluate mission rules against collected data
            missions_to_create = []
            for data_point in data_points:
                missions = await self._evaluate_mission_rules(data_point)
                missions_to_create.extend(missions)
            
            # Create missions on the platform
            missions_created = 0
            for mission_data in missions_to_create:
                result = await self._create_platform_mission(mission_data)
                if result["success"]:
                    missions_created += 1
            
            self._last_sync = datetime.now()
            LOGGER.info(f"Sync completed: {missions_created} missions created from {len(data_points)} data points")
            
        except Exception as e:
            error_info = {
                "timestamp": datetime.now().isoformat(),
                "error": str(e),
                "type": "sync_error"
            }
            self._sync_errors.append(error_info)
            self._sync_errors = self._sync_errors[-10:]  # Keep last 10 errors
            LOGGER.error(f"Sync failed: {e}")
            self._mission_stats["error_count"] += 1

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
                "platform_type": self._platform_config.get("type", "unknown"),
                "base_url": self._platform_config.get("base_url", "unknown")
            }
        except Exception as e:
            return {"authenticated": False, "error": str(e)}

    async def _test_connection(self) -> Dict[str, Any]:
        """Test platform API connection."""
        try:
            if not await self._authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            # Test API call
            endpoint = self._platform_config.get("endpoints", {}).get("health", "/health")
            url = f"{self._platform_config['base_url']}{endpoint}"
            
            response = self._session.get(url, timeout=10)
            
            return {
                "success": response.status_code == 200,
                "status_code": response.status_code,
                "response_time": response.elapsed.total_seconds(),
                "platform_type": self._platform_config.get("type", "unknown")
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _create_mission_command(self, command: Dict) -> Dict[str, Any]:
        """Create mission via command."""
        try:
            mission_data = {
                "title": command.get("title", "Manual Mission"),
                "type": command.get("type", "manual"),
                "store_id": command.get("store_id", "unknown"),
                "due_date": command.get("due_date", (datetime.now() + timedelta(hours=24)).isoformat()),
                "priority": command.get("priority", "medium"),
                "custom_fields": command.get("custom_fields", {})
            }
            
            result = await self._create_platform_mission(mission_data)
            return result
            
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _get_missions_command(self, command: Dict) -> Dict[str, Any]:
        """Get missions via command."""
        try:
            if not await self._authenticate():
                return {"success": False, "error": "Authentication failed"}
            
            endpoint = self._platform_config.get("endpoints", {}).get("get_missions", "/missions")
            url = f"{self._platform_config['base_url']}{endpoint}"
            
            params = {}
            if command.get("store_id"):
                params["store_id"] = command["store_id"]
            if command.get("limit"):
                params["limit"] = command["limit"]
            
            response = self._session.get(url, params=params, timeout=self._timeout)
            
            if response.status_code == 200:
                data = response.json()
                return {
                    "success": True,
                    "missions": data.get("data", []),
                    "count": data.get("count", 0)
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
            if not mission_id:
                return {"success": False, "error": "mission_id required"}
            
            endpoint = self._platform_config.get("endpoints", {}).get("validate_mission", f"/missions/{mission_id}/validate")
            url = f"{self._platform_config['base_url']}{endpoint}"
            
            validation_data = {
                "compliant": command.get("compliant", True),
                "rating": command.get("rating", 5),
                "reason_noncompliant": command.get("reason_noncompliant"),
                "validation_data": command.get("validation_data", {})
            }
            
            response = self._session.post(url, json=validation_data, timeout=self._timeout)
            
            if response.status_code in [200, 201]:
                self._mission_stats["missions_completed"] += 1
                return {"success": True, "validated": validation_data["compliant"]}
            else:
                return {"success": False, "error": f"Validation failed: {response.status_code}"}
                
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def _get_stats(self) -> Dict[str, Any]:
        """Get mission statistics."""
        uptime = (datetime.now() - self._mission_stats["uptime_start"]).total_seconds()
        
        return {
            "success": True,
            "stats": {
                **self._mission_stats,
                "uptime_seconds": round(uptime, 1),
                "error_rate": self._mission_stats["error_count"] / max(1, self._mission_stats["sync_count"]),
                "recent_errors": self._sync_errors[-5:]
            }
        }

    async def _clear_errors(self) -> Dict[str, Any]:
        """Clear error log."""
        self._sync_errors.clear()
        return {"success": True, "message": "Errors cleared"}

    async def _evaluate_rules_command(self, command: Dict) -> Dict[str, Any]:
        """Evaluate rules against provided data."""
        try:
            data = command.get("data", {})
            missions = await self._evaluate_mission_rules(data)
            
            return {
                "success": True,
                "missions_to_create": missions,
                "count": len(missions)
            }
        except Exception as e:
            return {"success": False, "error": str(e)}

    async def close(self):
        """Clean up resources."""
        LOGGER.info(f"Shutting down mission manager: {self.name}")
        self._stop_sync_task()
        
        if self._session:
            self._session.close()
            
        await super().close()

    async def get_geometries(
        self, *, extra: Optional[Dict[str, Any]] = None, timeout: Optional[float] = None
    ) -> List[Geometry]:
        """Return empty geometries (this is a data integration component)."""
        return []