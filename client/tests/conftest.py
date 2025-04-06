"""
File: client/tests/conftest.py
Configuration for pytest integration tests.
"""
import os
import pytest

def pytest_addoption(parser):
    """Add command line options to pytest."""
    parser.addoption(
        "--debug-level",
        default="basic",
        choices=["basic", "advanced", "trace"],
        help="Set debug level for tests",
    )

def pytest_configure(config):
    """Configure pytest with command line options and set values to be accessible in tests."""
    # Get debug level from command line option
    debug_level = config.getoption("--debug-level")
    
    # Configure for use in tests
    pytest.debug_level = debug_level
    
    # Set environment variable for consistency
    os.environ["DEBUG_LEVEL"] = debug_level
    
    # Print configuration for reference
    print(f"\nConfiguring tests with DEBUG_LEVEL={debug_level}\n")