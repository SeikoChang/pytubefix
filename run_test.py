import pytest
import os
import sys

# Ensure the current directory is added to sys.path
# This is often needed when running pytest from within a notebook context
if os.getcwd() not in sys.path:
    sys.path.insert(0, os.getcwd())

# Run pytest tests from the generated test file
# The -v flag provides verbose output
# The -s flag allows print statements to show up
pytest.main(["-v", "test_youtube_downloader.py"])
