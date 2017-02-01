

import pytest
import os

current_path = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def source_dsn():
    return 'postgresql://test:test@localhost:5432/lustro_source'


@pytest.fixture
def target_dsn():
    return 'postgresql://test:test@localhost:5432/lustro_target'
