"""
Aplicación Flask para gestión de contenedores CCIS.
"""

from flask import Flask
from flask_sqlalchemy import SQLAlchemy

db = SQLAlchemy()


def create_app(config_class=None):
    """Factory function para crear la aplicación Flask."""
    app = Flask(__name__, 
                static_folder='../static', 
                template_folder='../templates')
    
    # Configuración por defecto
    if config_class:
        app.config.from_object(config_class)
    else:
        app.secret_key = 'es-secreto'
        app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
        app.config['SQLALCHEMY_TRACK_MODIFICATIONS'] = False
    
    # Inicializar extensiones
    db.init_app(app)
    
    # Registrar blueprints
    from .routes import main_bp
    app.register_blueprint(main_bp)
    
    # Crear tablas de base de datos
    with app.app_context():
        from .models import Contenedor
        db.create_all()
    
    return app
