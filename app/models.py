"""
Modelos de la base de datos para la aplicación CCIS.
"""

from . import db


class Contenedor(db.Model):
    """Modelo para representar un contenedor."""
    __tablename__ = 'contenedor'
    
    id = db.Column(db.Integer, primary_key=True)
    containerNo = db.Column(db.String(200), nullable=False, index=True)
    iso = db.Column(db.String(100), index=True)
    grado = db.Column(db.String(50), index=True)
    status = db.Column(db.String(50), index=True)
    days = db.Column(db.Integer)
    remark = db.Column(db.String(200))
    ofacc = db.Column(db.String(2), default='N', index=True)
    block = db.Column(db.String(2), default='N', index=True)
    traslado = db.Column(db.String(2), default='N', index=True)
    active = db.Column(db.String(2), default='Y', index=True)
    
    def __repr__(self):
        return f'<Contenedor {self.containerNo}>'
