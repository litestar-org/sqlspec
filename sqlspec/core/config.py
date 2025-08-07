"""Enhanced configuration management for core SQLSpec system.

This module provides centralized configuration management that consolidates
and enhances the current configuration system while maintaining complete
backward compatibility.

Key Enhancements:
- Centralized configuration for all core components
- Performance-optimized configuration access with caching
- Environment variable integration for deployment flexibility
- Configuration validation and type checking
- Thread-safe configuration updates for runtime changes

Architecture:
- CoreConfig: Main configuration class with all settings
- ConfigManager: Singleton manager for global configuration access
- Configuration validation and type coercion
- Environment variable mapping with defaults
- Immutable configuration updates with replace() pattern

Performance Features:
- Cached configuration access for O(1) property lookup
- Lazy loading of expensive configuration validation
- Memory-efficient storage with __slots__
- Configuration change notifications for component updates

Compatibility Requirements:
- Same configuration attributes that existing code expects
- Identical behavior for StatementConfig and parameter configurations
- Same environment variable names and precedence
- Complete backward compatibility with existing configuration patterns
"""

import os
import threading
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Callable, Optional, Union

from sqlspec.core.parameters import ParameterStyle, ParameterStyleConfig
from sqlspec.utils.logging import get_logger

# Enable when MyPyC ready
# from mypy_extensions import mypyc_attr

__all__ = (
    "CacheConfiguration",
    "ConfigManager",
    "CoreConfig",
    "DatabaseConfiguration",
    "ProcessingConfiguration",
    "SecurityConfiguration",
    "create_default_config",
    "get_global_config",
    "load_config_from_env",
    "set_global_config",
    "validate_config",
)

logger = get_logger("sqlspec.core.config")


@dataclass(frozen=True)
class CacheConfiguration:
    """Cache system configuration - enhanced from existing cache settings.

    Consolidates all cache-related configuration into a single structure
    for the unified cache system.
    """

    enable_caching: bool = True
    max_compilation_cache_size: int = 1000
    max_parameter_cache_size: int = 500
    max_ast_cache_size: int = 200
    max_driver_cache_size: int = 300
    total_memory_limit_mb: int = 50
    enable_cache_stats: bool = True
    cache_eviction_threshold: float = 0.8


@dataclass(frozen=True)
class ProcessingConfiguration:
    """Processing pipeline configuration - enhanced from existing settings.

    Controls the behavior of the core processing pipeline with performance
    and compatibility settings.
    """

    enable_parsing: bool = True
    enable_validation: bool = True
    enable_single_pass: bool = True
    enable_mypy_c_optimizations: bool = False  # Disabled until MyPyC ready
    max_sql_length: int = 1024 * 1024  # 1MB max SQL size
    enable_ast_caching: bool = True
    enable_parameter_wrapping: bool = True


@dataclass(frozen=True)
class DatabaseConfiguration:
    """Database-specific configuration - consolidated from driver configs.

    Provides database-specific settings that affect processing behavior
    across all drivers.
    """

    default_dialect: str = "auto"
    enable_dialect_detection: bool = True
    connection_timeout: int = 30
    query_timeout: int = 300
    max_connection_pool_size: int = 20
    enable_prepared_statements: bool = True


@dataclass(frozen=True)
class SecurityConfiguration:
    """Security-related configuration - enhanced from existing security settings.

    Consolidates security settings including parameter handling, SQL injection
    prevention, and access control.
    """

    enable_parameter_validation: bool = True
    enable_sql_injection_detection: bool = True
    max_parameter_count: int = 1000
    max_parameter_size_bytes: int = 10 * 1024 * 1024  # 10MB
    enable_statement_whitelisting: bool = False
    allowed_statement_types: Optional[set[str]] = None


# @mypyc_attr(allow_interpreted_subclasses=True)  # Enable when MyPyC ready
class CoreConfig:
    """Enhanced core configuration with complete backward compatibility.

    Provides centralized configuration for all SQLSpec components while
    maintaining complete compatibility with existing configuration interfaces.

    Configuration Categories:
    - Cache: Unified cache system settings
    - Processing: SQL processing pipeline settings
    - Database: Database connection and execution settings
    - Security: Security and validation settings

    Performance Features:
    - __slots__ for memory efficiency
    - Cached property access for O(1) lookups
    - Lazy validation for expensive checks
    - Immutable updates with efficient copying

    Compatibility Features:
    - Same configuration attributes as existing StatementConfig
    - Environment variable mapping preservation
    - Same default values and behavior
    - Complete API compatibility for drivers and application code
    """

    __slots__ = (
        "_cache_config",
        "_config_hash",
        "_custom_settings",
        "_database_config",
        "_processing_config",
        "_security_config",
        "_validation_cache",
    )

    def __init__(
        self,
        cache_config: Optional[CacheConfiguration] = None,
        processing_config: Optional[ProcessingConfiguration] = None,
        database_config: Optional[DatabaseConfiguration] = None,
        security_config: Optional[SecurityConfiguration] = None,
        custom_settings: Optional[dict[str, Any]] = None,
    ) -> None:
        """Initialize core configuration with all settings.

        Args:
            cache_config: Cache system configuration
            processing_config: Processing pipeline configuration
            database_config: Database-specific configuration
            security_config: Security and validation configuration
            custom_settings: Additional custom settings
        """
        self._cache_config = cache_config or CacheConfiguration()
        self._processing_config = processing_config or ProcessingConfiguration()
        self._database_config = database_config or DatabaseConfiguration()
        self._security_config = security_config or SecurityConfiguration()
        self._custom_settings = custom_settings or {}
        
        # Pre-compute hash for performance
        self._config_hash = self._compute_config_hash()
        
        # Validation cache for expensive checks
        self._validation_cache: Optional[list[str]] = None

    def _compute_config_hash(self) -> int:
        """Compute configuration hash for fast equality checks."""
        return hash((
            hash(self._cache_config) if hasattr(self._cache_config, '__dict__') else id(self._cache_config),
            hash(self._processing_config) if hasattr(self._processing_config, '__dict__') else id(self._processing_config),
            hash(self._database_config) if hasattr(self._database_config, '__dict__') else id(self._database_config),
            hash(self._security_config) if hasattr(self._security_config, '__dict__') else id(self._security_config),
            tuple(sorted(self._custom_settings.items())) if self._custom_settings else ()
        ))

    # Cache Configuration Properties
    @property
    def enable_caching(self) -> bool:
        """Enable caching - preserved interface."""
        return self._cache_config.enable_caching

    @property
    def cache_config(self) -> CacheConfiguration:
        """Complete cache configuration."""
        return self._cache_config

    # Processing Configuration Properties
    @property
    def enable_parsing(self) -> bool:
        """Enable parsing - preserved interface."""
        return self._processing_config.enable_parsing

    @property
    def enable_validation(self) -> bool:
        """Enable validation - preserved interface."""
        return self._processing_config.enable_validation

    @property
    def enable_single_pass(self) -> bool:
        """Enable single-pass processing optimization."""
        return self._processing_config.enable_single_pass

    @property
    def processing_config(self) -> ProcessingConfiguration:
        """Complete processing configuration."""
        return self._processing_config

    # Database Configuration Properties
    @property
    def dialect(self) -> str:
        """SQL dialect - preserved interface."""
        return self._database_config.default_dialect

    @property
    def database_config(self) -> DatabaseConfiguration:
        """Complete database configuration."""
        return self._database_config

    # Security Configuration Properties
    @property
    def security_config(self) -> SecurityConfiguration:
        """Complete security configuration."""
        return self._security_config

    # Custom Settings Access
    def get_setting(self, key: str, default: Any = None) -> Any:
        """Get custom setting value - extensible interface.

        Args:
            key: Setting key
            default: Default value if not found

        Returns:
            Setting value or default
        """
        return self._custom_settings.get(key, default)

    def set_setting(self, key: str, value: Any) -> "CoreConfig":
        """Create new configuration with updated custom setting.

        Args:
            key: Setting key
            value: Setting value

        Returns:
            New CoreConfig instance with updated setting
        """
        new_custom_settings = self._custom_settings.copy()
        new_custom_settings[key] = value
        return self.replace(custom_settings=new_custom_settings)

    # Configuration Updates (Immutable Pattern)
    def replace(self, **kwargs) -> "CoreConfig":
        """Create new configuration with updated values - preserved pattern.

        Provides immutable update pattern compatible with existing StatementConfig.replace().

        Args:
            **kwargs: Configuration values to update

        Returns:
            New CoreConfig instance with updated values
        """
        # Extract configuration updates
        cache_config = kwargs.pop("cache_config", self._cache_config)
        processing_config = kwargs.pop("processing_config", self._processing_config)
        database_config = kwargs.pop("database_config", self._database_config)
        security_config = kwargs.pop("security_config", self._security_config)
        custom_settings = kwargs.pop("custom_settings", self._custom_settings)

        # Handle individual setting updates for backward compatibility
        if kwargs:
            # Create new custom settings with updates
            new_custom_settings = custom_settings.copy()
            new_custom_settings.update(kwargs)
            custom_settings = new_custom_settings

        return CoreConfig(
            cache_config=cache_config,
            processing_config=processing_config,
            database_config=database_config,
            security_config=security_config,
            custom_settings=custom_settings,
        )

    # Configuration Validation
    def validate(self) -> "list[str]":
        """Validate configuration consistency - enhanced functionality.

        Returns:
            List of validation errors (empty if valid)
        """
        if self._validation_cache is not None:
            return self._validation_cache.copy()

        errors = []
        
        # Validate cache configuration
        if self._cache_config.max_compilation_cache_size <= 0:
            errors.append("max_compilation_cache_size must be positive")
        if self._cache_config.total_memory_limit_mb <= 0:
            errors.append("total_memory_limit_mb must be positive")
        if not (0.0 <= self._cache_config.cache_eviction_threshold <= 1.0):
            errors.append("cache_eviction_threshold must be between 0.0 and 1.0")

        # Validate processing configuration
        if self._processing_config.max_sql_length <= 0:
            errors.append("max_sql_length must be positive")

        # Validate database configuration
        if self._database_config.connection_timeout <= 0:
            errors.append("connection_timeout must be positive")
        if self._database_config.query_timeout <= 0:
            errors.append("query_timeout must be positive")
        if self._database_config.max_connection_pool_size <= 0:
            errors.append("max_connection_pool_size must be positive")

        # Validate security configuration
        if self._security_config.max_parameter_count <= 0:
            errors.append("max_parameter_count must be positive")
        if self._security_config.max_parameter_size_bytes <= 0:
            errors.append("max_parameter_size_bytes must be positive")

        # Cache validation results
        self._validation_cache = errors.copy()
        return errors

    # Performance Optimization
    def __hash__(self) -> int:
        """Cached hash for configuration equality checks."""
        return self._config_hash

    def __eq__(self, other: object) -> bool:
        """Efficient equality comparison with hash short-circuit."""
        if not isinstance(other, CoreConfig):
            return False
        if self._config_hash != other._config_hash:
            return False
        return (
            self._cache_config == other._cache_config
            and self._processing_config == other._processing_config
            and self._database_config == other._database_config
            and self._security_config == other._security_config
            and self._custom_settings == other._custom_settings
        )

    def __repr__(self) -> str:
        """String representation of core configuration."""
        return (
            f"CoreConfig("
            f"cache_config={self._cache_config!r}, "
            f"processing_config={self._processing_config!r}, "
            f"database_config={self._database_config!r}, "
            f"security_config={self._security_config!r}, "
            f"custom_settings={self._custom_settings!r})"
        )


class ConfigManager:
    """Singleton configuration manager for global configuration access.

    Provides thread-safe access to global configuration with change
    notification and validation capabilities.

    Features:
    - Thread-safe configuration access and updates
    - Configuration change notifications for component updates
    - Environment variable integration with precedence rules
    - Configuration validation and error reporting
    - Hot configuration reloading for runtime changes
    """

    __slots__ = ("_change_callbacks", "_config", "_lock")

    def __init__(self) -> None:
        """Initialize configuration manager."""
        self._config = create_default_config()
        self._lock = threading.RLock()
        self._change_callbacks: list[Callable[[CoreConfig], None]] = []

    def get_config(self) -> CoreConfig:
        """Get current global configuration - thread-safe.

        Returns:
            Current CoreConfig instance
        """
        with self._lock:
            return self._config

    def set_config(self, config: CoreConfig) -> None:
        """Set global configuration - thread-safe with notifications.

        Args:
            config: New configuration to set
        """
        # Validate configuration before setting
        validation_errors = config.validate()
        if validation_errors:
            error_msg = f"Invalid configuration: {', '.join(validation_errors)}"
            raise ValueError(error_msg)

        with self._lock:
            old_config = self._config
            self._config = config

            # Notify change callbacks
            for callback in self._change_callbacks:
                try:
                    callback(config)
                except Exception as e:
                    logger.warning("Configuration change callback failed: %s", e)

        logger.info("Global configuration updated")

    def add_change_callback(self, callback: Callable[[CoreConfig], None]) -> None:
        """Add callback for configuration changes.

        Args:
            callback: Function to call when configuration changes
        """
        with self._lock:
            self._change_callbacks.append(callback)

    def remove_change_callback(self, callback: Callable[[CoreConfig], None]) -> bool:
        """Remove configuration change callback.

        Args:
            callback: Callback function to remove

        Returns:
            True if callback was found and removed, False otherwise
        """
        with self._lock:
            try:
                self._change_callbacks.remove(callback)
                return True
            except ValueError:
                return False

    def reload_from_env(self) -> None:
        """Reload configuration from environment variables.

        Reloads configuration from environment variables while preserving
        any programmatically set values that have higher precedence.
        """
        env_config = load_config_from_env()
        
        # Merge with current custom settings (preserve programmatic changes)
        current_custom = self._config._custom_settings.copy()
        new_config = env_config.replace(custom_settings={
            **env_config._custom_settings,
            **current_custom  # Programmatic settings have higher precedence
        })
        
        self.set_config(new_config)

    def update_config(self, **kwargs) -> None:
        """Update current configuration with new values.

        Args:
            **kwargs: Configuration values to update
        """
        with self._lock:
            new_config = self._config.replace(**kwargs)
            self.set_config(new_config)

    def reset_to_defaults(self) -> None:
        """Reset configuration to default values."""
        default_config = create_default_config()
        self.set_config(default_config)


# Global configuration manager instance
_config_manager: Optional[ConfigManager] = None
_config_lock = threading.Lock()


def _get_config_manager() -> ConfigManager:
    """Get or create the global configuration manager."""
    global _config_manager
    if _config_manager is None:
        with _config_lock:
            if _config_manager is None:
                _config_manager = ConfigManager()
    return _config_manager


def get_global_config() -> CoreConfig:
    """Get global configuration instance - preserved interface.

    Returns:
        Global CoreConfig instance
    """
    manager = _get_config_manager()
    return manager.get_config()


def set_global_config(config: CoreConfig) -> None:
    """Set global configuration - preserved interface.

    Args:
        config: New configuration to set globally
    """
    manager = _get_config_manager()
    manager.set_config(config)


def load_config_from_env() -> CoreConfig:
    """Load configuration from environment variables - enhanced functionality.

    Environment Variables Supported:
    - SQLSPEC_ENABLE_CACHING: Enable/disable caching (true/false)
    - SQLSPEC_MAX_CACHE_SIZE: Maximum cache size (integer)
    - SQLSPEC_CACHE_MEMORY_LIMIT: Cache memory limit in MB (integer)
    - SQLSPEC_DIALECT: Default SQL dialect (string)
    - SQLSPEC_ENABLE_VALIDATION: Enable parameter validation (true/false)
    - SQLSPEC_MAX_SQL_LENGTH: Maximum SQL statement length (integer)
    - SQLSPEC_CONNECTION_TIMEOUT: Database connection timeout (integer)
    - SQLSPEC_QUERY_TIMEOUT: Database query timeout (integer)
    - SQLSPEC_MAX_PARAMETERS: Maximum number of parameters (integer)

    Returns:
        CoreConfig loaded from environment variables
    """
    # Cache configuration from environment
    cache_config = CacheConfiguration(
        enable_caching=_env_bool("SQLSPEC_ENABLE_CACHING", True),
        max_compilation_cache_size=_env_int("SQLSPEC_MAX_CACHE_SIZE", 1000),
        total_memory_limit_mb=_env_int("SQLSPEC_CACHE_MEMORY_LIMIT", 50),
        enable_cache_stats=_env_bool("SQLSPEC_ENABLE_CACHE_STATS", True),
    )

    # Processing configuration from environment
    processing_config = ProcessingConfiguration(
        enable_parsing=_env_bool("SQLSPEC_ENABLE_PARSING", True),
        enable_validation=_env_bool("SQLSPEC_ENABLE_VALIDATION", True),
        enable_single_pass=_env_bool("SQLSPEC_ENABLE_SINGLE_PASS", True),
        max_sql_length=_env_int("SQLSPEC_MAX_SQL_LENGTH", 1024 * 1024),
        enable_ast_caching=_env_bool("SQLSPEC_ENABLE_AST_CACHING", True),
    )

    # Database configuration from environment
    database_config = DatabaseConfiguration(
        default_dialect=os.getenv("SQLSPEC_DIALECT", "auto"),
        enable_dialect_detection=_env_bool("SQLSPEC_ENABLE_DIALECT_DETECTION", True),
        connection_timeout=_env_int("SQLSPEC_CONNECTION_TIMEOUT", 30),
        query_timeout=_env_int("SQLSPEC_QUERY_TIMEOUT", 300),
        max_connection_pool_size=_env_int("SQLSPEC_MAX_POOL_SIZE", 20),
        enable_prepared_statements=_env_bool("SQLSPEC_ENABLE_PREPARED_STATEMENTS", True),
    )

    # Security configuration from environment
    security_config = SecurityConfiguration(
        enable_parameter_validation=_env_bool("SQLSPEC_ENABLE_PARAM_VALIDATION", True),
        enable_sql_injection_detection=_env_bool("SQLSPEC_ENABLE_INJECTION_DETECTION", True),
        max_parameter_count=_env_int("SQLSPEC_MAX_PARAMETERS", 1000),
        max_parameter_size_bytes=_env_int("SQLSPEC_MAX_PARAM_SIZE", 10 * 1024 * 1024),
        enable_statement_whitelisting=_env_bool("SQLSPEC_ENABLE_STATEMENT_WHITELIST", False),
    )

    return CoreConfig(
        cache_config=cache_config,
        processing_config=processing_config,
        database_config=database_config,
        security_config=security_config,
    )


def validate_config(config: CoreConfig) -> "list[str]":
    """Validate configuration completeness and consistency - enhanced functionality.

    Args:
        config: Configuration to validate

    Returns:
        List of validation errors (empty if valid)
    """
    return config.validate()


def create_default_config() -> CoreConfig:
    """Create default configuration - preserved interface.

    Returns:
        CoreConfig with default values for all settings
    """
    return CoreConfig(
        cache_config=CacheConfiguration(),
        processing_config=ProcessingConfiguration(),
        database_config=DatabaseConfiguration(),
        security_config=SecurityConfiguration(),
    )


def _env_bool(key: str, default: bool) -> bool:
    """Get boolean value from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    return value.lower() in ("true", "1", "yes", "on", "enabled")


def _env_int(key: str, default: int) -> int:
    """Get integer value from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return int(value)
    except ValueError:
        logger.warning("Invalid integer value for %s: %s, using default %d", key, value, default)
        return default


def _env_float(key: str, default: float) -> float:
    """Get float value from environment variable."""
    value = os.getenv(key)
    if value is None:
        return default
    try:
        return float(value)
    except ValueError:
        logger.warning("Invalid float value for %s: %s, using default %f", key, value, default)
        return default


# Configuration file support
def load_config_from_file(config_path: Union[str, Path]) -> CoreConfig:
    """Load configuration from YAML/JSON file - enhanced functionality.

    Args:
        config_path: Path to configuration file

    Returns:
        CoreConfig loaded from file

    Raises:
        FileNotFoundError: If config file doesn't exist
        ValueError: If config file format is invalid
    """
    import json
    from pathlib import Path as PathlibPath

    config_path = PathlibPath(config_path)
    
    if not config_path.exists():
        raise FileNotFoundError(f"Configuration file not found: {config_path}")

    try:
        # Try JSON first, then YAML if available
        if config_path.suffix.lower() == '.json':
            with open(config_path, 'r', encoding='utf-8') as f:
                config_dict = json.load(f)
        elif config_path.suffix.lower() in ('.yaml', '.yml'):
            try:
                import yaml
                with open(config_path, 'r', encoding='utf-8') as f:
                    config_dict = yaml.safe_load(f)
            except ImportError as e:
                raise ValueError("YAML support requires PyYAML: pip install PyYAML") from e
        else:
            # Auto-detect format
            with open(config_path, 'r', encoding='utf-8') as f:
                content = f.read()
                try:
                    config_dict = json.loads(content)
                except json.JSONDecodeError:
                    try:
                        import yaml
                        config_dict = yaml.safe_load(content)
                    except ImportError as e:
                        raise ValueError("Unable to parse config file format") from e

        return _config_dict_to_core_config(config_dict)

    except (json.JSONDecodeError, Exception) as e:
        raise ValueError(f"Invalid configuration file format: {e}") from e


def save_config_to_file(config: CoreConfig, config_path: Union[str, Path]) -> None:
    """Save configuration to YAML/JSON file - enhanced functionality.

    Args:
        config: Configuration to save
        config_path: Path to save configuration file
    """
    import json
    from pathlib import Path as PathlibPath

    config_path = PathlibPath(config_path)
    config_dict = _core_config_to_dict(config)

    # Ensure parent directory exists
    config_path.parent.mkdir(parents=True, exist_ok=True)

    if config_path.suffix.lower() == '.json':
        with open(config_path, 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, sort_keys=True)
    elif config_path.suffix.lower() in ('.yaml', '.yml'):
        try:
            import yaml
            with open(config_path, 'w', encoding='utf-8') as f:
                yaml.safe_dump(config_dict, f, default_flow_style=False, sort_keys=True)
        except ImportError as e:
            raise ValueError("YAML support requires PyYAML: pip install PyYAML") from e
    else:
        # Default to JSON
        with open(config_path.with_suffix('.json'), 'w', encoding='utf-8') as f:
            json.dump(config_dict, f, indent=2, sort_keys=True)


def _config_dict_to_core_config(config_dict: dict[str, Any]) -> CoreConfig:
    """Convert configuration dictionary to CoreConfig instance."""
    # Extract sub-configurations
    cache_dict = config_dict.get('cache', {})
    processing_dict = config_dict.get('processing', {})
    database_dict = config_dict.get('database', {})
    security_dict = config_dict.get('security', {})
    custom_dict = config_dict.get('custom', {})

    return CoreConfig(
        cache_config=CacheConfiguration(**cache_dict),
        processing_config=ProcessingConfiguration(**processing_dict),
        database_config=DatabaseConfiguration(**database_dict),
        security_config=SecurityConfiguration(**security_dict),
        custom_settings=custom_dict,
    )


def _core_config_to_dict(config: CoreConfig) -> dict[str, Any]:
    """Convert CoreConfig instance to dictionary for serialization."""
    return {
        'cache': {
            'enable_caching': config._cache_config.enable_caching,
            'max_compilation_cache_size': config._cache_config.max_compilation_cache_size,
            'max_parameter_cache_size': config._cache_config.max_parameter_cache_size,
            'max_ast_cache_size': config._cache_config.max_ast_cache_size,
            'max_driver_cache_size': config._cache_config.max_driver_cache_size,
            'total_memory_limit_mb': config._cache_config.total_memory_limit_mb,
            'enable_cache_stats': config._cache_config.enable_cache_stats,
            'cache_eviction_threshold': config._cache_config.cache_eviction_threshold,
        },
        'processing': {
            'enable_parsing': config._processing_config.enable_parsing,
            'enable_validation': config._processing_config.enable_validation,
            'enable_single_pass': config._processing_config.enable_single_pass,
            'enable_mypy_c_optimizations': config._processing_config.enable_mypy_c_optimizations,
            'max_sql_length': config._processing_config.max_sql_length,
            'enable_ast_caching': config._processing_config.enable_ast_caching,
            'enable_parameter_wrapping': config._processing_config.enable_parameter_wrapping,
        },
        'database': {
            'default_dialect': config._database_config.default_dialect,
            'enable_dialect_detection': config._database_config.enable_dialect_detection,
            'connection_timeout': config._database_config.connection_timeout,
            'query_timeout': config._database_config.query_timeout,
            'max_connection_pool_size': config._database_config.max_connection_pool_size,
            'enable_prepared_statements': config._database_config.enable_prepared_statements,
        },
        'security': {
            'enable_parameter_validation': config._security_config.enable_parameter_validation,
            'enable_sql_injection_detection': config._security_config.enable_sql_injection_detection,
            'max_parameter_count': config._security_config.max_parameter_count,
            'max_parameter_size_bytes': config._security_config.max_parameter_size_bytes,
            'enable_statement_whitelisting': config._security_config.enable_statement_whitelisting,
            'allowed_statement_types': list(config._security_config.allowed_statement_types) if config._security_config.allowed_statement_types else None,
        },
        'custom': config._custom_settings,
    }


# Configuration management helpers
def get_config_manager() -> ConfigManager:
    """Get the global configuration manager instance.
    
    Returns:
        Global ConfigManager instance
    """
    return _get_config_manager()


def add_global_change_callback(callback: Callable[[CoreConfig], None]) -> None:
    """Add global configuration change callback.
    
    Args:
        callback: Function to call when global configuration changes
    """
    manager = _get_config_manager()
    manager.add_change_callback(callback)


def remove_global_change_callback(callback: Callable[[CoreConfig], None]) -> bool:
    """Remove global configuration change callback.
    
    Args:
        callback: Callback function to remove
        
    Returns:
        True if callback was found and removed, False otherwise
    """
    manager = _get_config_manager()
    return manager.remove_change_callback(callback)


# Implementation status tracking
__module_status__ = "IMPLEMENTED"  # PLACEHOLDER → BUILDING → TESTING → COMPLETE
__enhancement_target__ = "Centralized Configuration"  # Primary enhancement goal
__compatibility_target__ = "100%"  # Must maintain complete compatibility
