"""Example of how to configure logging for SQLSpec.

Since SQLSpec no longer provides a configure_logging function,
users can set up their own logging configuration as needed.
"""

import logging
import sys

from sqlspec.utils.correlation import correlation_context
from sqlspec.utils.logging import StructuredFormatter, get_logger

__all__ = ("demo_correlation_ids", "setup_advanced_logging", "setup_simple_logging", "setup_structured_logging")


# Example 1: Basic logging setup with structured JSON output
def setup_structured_logging() -> None:
    """Set up structured JSON logging for SQLSpec."""
    # Get the SQLSpec logger
    sqlspec_logger = logging.getLogger("sqlspec")

    # Set the logging level
    sqlspec_logger.setLevel(logging.INFO)

    # Create a console handler with structured formatter
    console_handler = logging.StreamHandler(sys.stdout)
    console_handler.setFormatter(StructuredFormatter())

    # Add the handler to the logger
    sqlspec_logger.addHandler(console_handler)

    # Don't propagate to the root logger
    sqlspec_logger.propagate = False

    print("Structured logging configured for SQLSpec")


# Example 2: Simple text logging
def setup_simple_logging() -> None:
    """Set up simple text logging for SQLSpec."""
    # Configure basic logging for the entire application
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
        handlers=[logging.StreamHandler(sys.stdout)],
    )

    print("Simple logging configured")


# Example 3: Advanced setup with file output and custom formatting
def setup_advanced_logging() -> None:
    """Set up advanced logging with multiple handlers."""
    sqlspec_logger = logging.getLogger("sqlspec")
    sqlspec_logger.setLevel(logging.DEBUG)

    # Console handler with simple format
    console_handler = logging.StreamHandler(sys.stdout)
    console_formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    console_handler.setFormatter(console_formatter)
    console_handler.setLevel(logging.INFO)  # Only INFO and above to console

    # File handler with structured format
    file_handler = logging.FileHandler("sqlspec.log")
    file_handler.setFormatter(StructuredFormatter())
    file_handler.setLevel(logging.DEBUG)  # All messages to file

    # Add both handlers
    sqlspec_logger.addHandler(console_handler)
    sqlspec_logger.addHandler(file_handler)

    # Don't propagate to avoid duplicate logs
    sqlspec_logger.propagate = False

    print("Advanced logging configured with console and file output")


# Example 4: Using correlation IDs
def demo_correlation_ids() -> None:
    """Demonstrate using correlation IDs with logging."""

    logger = get_logger("example")

    # Without correlation ID
    logger.info("This log has no correlation ID")

    # With correlation ID
    with correlation_context() as correlation_id:
        logger.info("Starting operation with correlation ID: %s", correlation_id)
        logger.info("This log will include the correlation ID automatically")

        # Simulate some work
        logger.debug("Processing data...")
        logger.info("Operation completed")


if __name__ == "__main__":
    # Choose your logging setup
    print("=== Structured Logging Example ===")
    setup_structured_logging()
    demo_correlation_ids()

    print("\n=== Simple Logging Example ===")
    setup_simple_logging()

    print("\n=== Advanced Logging Example ===")
    setup_advanced_logging()
