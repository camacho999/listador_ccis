"""Pruebas unitarias para la aplicación CCIS."""

import pytest
from app import create_app
from app.models import Contenedor
from app import db


@pytest.fixture
def app():
    """Crear aplicación para pruebas."""
    app = create_app()
    app.config['TESTING'] = True
    app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///:memory:'
    
    with app.app_context():
        db.create_all()
        yield app
        db.session.remove()
        db.drop_all()


@pytest.fixture
def client(app):
    """Cliente de prueba."""
    return app.test_client()


def test_home_page(client):
    """Prueba página principal."""
    response = client.get('/')
    assert response.status_code == 200


def test_cargar_page(client):
    """Prueba página de carga."""
    response = client.get('/cargar')
    assert response.status_code == 200


def test_generar_page(client):
    """Prueba página generador."""
    response = client.get('/generar')
    assert response.status_code == 200


def test_filtro_route(client):
    """Prueba ruta de filtrado."""
    response = client.get('/filtro')
    assert response.status_code == 200


def test_contenedor_model(app):
    """Prueba modelo Contenedor."""
    with app.app_context():
        container = Contenedor(
            containerNo='TEST123',
            iso='22G1',
            grado='GC',
            status='AV',
            days=5,
            remark='Test'
        )
        db.session.add(container)
        db.session.commit()
        
        result = Contenedor.query.filter_by(containerNo='TEST123').first()
        assert result is not None
        assert result.iso == '22G1'
        assert result.grado == 'GC'
