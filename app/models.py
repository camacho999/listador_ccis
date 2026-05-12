"""
Modelos de la base de datos para la aplicación CCIS.
"""

from . import db


class Contenedor(db.Model):
    """Modelo para representar un contenedor."""
    __tablename__ = 'contenedor'
    
    id = db.Column(db.Integer, primary_key=True)
    containerNo = db.Column(db.String(200), nullable=False)
    iso = db.Column(db.String(100))
    grado = db.Column(db.String(50))
    status = db.Column(db.String(50))
    days = db.Column(db.Integer)
    remark = db.Column(db.String(200))
    ofacc = db.Column(db.String(2), default='N')
    block = db.Column(db.String(2), default='N')
    traslado = db.Column(db.String(2), default='N')
    active = db.Column(db.String(2), default='Y')
    
    def __repr__(self):
        return f'<Contenedor {self.containerNo}>'
