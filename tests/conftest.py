from sqlalchemy import create_engine
import pytest
import os

from .user import Base
current_path = os.path.dirname(os.path.abspath(__file__))


@pytest.fixture
def source_dsn():
    return 'postgresql://test:test@localhost:5432/lustro_source'


@pytest.fixture
def sqllite_db():
    engine = create_engine('sqlite:///:memory:', echo=True)
    Base.metadata.create_all(engine)
    return engine


@pytest.fixture
def target_dsn():
    return 'postgresql://test:test@localhost:5432/lustro_target'
