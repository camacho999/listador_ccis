from flask import Flask, render_template,request, redirect, url_for, flash
from flask_sqlalchemy import SQLAlchemy
import pandas as pd 
import os

app = Flask(__name__)
app.secret_key = 'es-secreto'

app.config['SQLALCHEMY_DATABASE_URI'] = 'sqlite:///database.db'
app.config['SQLALCHEMY_TRACK_MODIFICATION'] = False 
db = SQLAlchemy(app)

class Contenedor(db.Model):
	id = db.Column(db.Integer, primary_key= True)
	containerNo = db.Column(db.String(200))
	iso    = db.Column(db.String(100))
	grado  = db.Column(db.String(50))
	status = db.Column(db.String(50))
	days   = db.Column(db.Integer())
	remark = db.Column(db.String(200))
	ofacc  = db.Column(db.String(2), default = 'N')
	block  = db.Column(db.String(2), default = 'N')
	traslado = db.Column(db.String(2), default = 'N')
	active = db.Column(db.String(2), default = 'Y')


with app.app_context():
	db.create_all()



@app.route('/')
def home():
	contenedores = Contenedor.query.all()
	return render_template('home.html', containers = contenedores)

@app.route('/cargar')
def cargar():
	return render_template('carga.html')

@app.route('/generar')
def generar():
	containers = Contenedor.query.all()
	return render_template('datos.html', containers = containers)

#Actualizando Base de Datos.
@app.route('/upload_inventario', methods = ['POST'])
def datos():
	if request.method == 'POST':
		container_db = Contenedor.query.all()
		archivo = request.files['archivo']
		if archivo and archivo.filename.endswith('.xls'):
			try:
				if container_db:
					print("Actualizando inventario")
					df = pd.read_excel(archivo)
					for _, fila in df.iterrows():
						exist = Contenedor.query.filter_by(containerNo = fila['Container No']).first()
						if exist:
							print('Contenedor en inventario')
						else:
							new_container = Contenedor(containerNo = fila['Container No'], iso = fila['ISO'], grado = fila['Grade'], status = fila['Sts'], days = fila['Days'], remark = fila['Remarks'])
							print(f'Agregando nuevo contenedor {new_container.containerNo}')
							db.session.add(new_container)
							db.session.commit()

					#Actualización de contenedores
					excel_container = set(df['Container No'].astype(str).tolist())
					db_actual = {container.containerNo for container in container_db}

					desactivate = db_actual - excel_container
					Contenedor.query.filter(Contenedor.containerNo.in_(desactivate)).update(
						{'active': 'N'},
						synchronize_session = False)
					db.session.commit()
					print(f'Se desactuvaron los contenedores que ya no esten en la base de datos {len(desactivate)}')


					print('Actualización Finalizada')
					flash(f'Actualizacion de inventario completada')
				else:
					print("Creando base de datos.")
					print('leyendo archivo')
					df = pd.read_excel(archivo)
					print(df)
					for _, fila in df.iterrows():
						print(fila)
						nuevo_contenedor = Contenedor(containerNo = fila['Container No'], iso = fila['ISO'], grado = fila['Grade'], status = fila['Sts'], days = fila['Days'], remark = fila['Remarks'])
						print(f'Agregando contenedor {nuevo_contenedor.containerNo}')
						db.session.add(nuevo_contenedor)
						db.session.commit()
					flash(f'Datos cargados con exito')

					


			except Exception as e:
				print(e)
				flash(f'Error al realizar la carga masiva: {str(e)}', 'danger')

			return redirect(url_for('cargar'))

				
				
				
		else:
			flash(f'El formato del archivo no es valido, solo se permite archivos xls.')
		return redirect(url_for('cargar'))	
	return render_template('datos.html', contenedores = contenedores)	


#Actualización bloqueados
@app.route('/update_blok', methods = ['POST'])
def bloqueados():
	if request.method == 'POST':
		archivo = request.files['archivo']
		if archivo and archivo.filename.endswith('.xls'):
			print("archivo aceptado")
			df = pd.read_excel(archivo)
			print("leyendo archivo")
			for _, fila in df.iterrows():
				print(f'Buscando contenedor {fila['Container']}')
				container = Contenedor.query.filter_by(containerNo = fila['Container']).first()
				if container:
					print(f'El Contenedor {container.containerNo} sera actualizado a estatus bloqueado')
					container.block = 'Y'
					db.session.commit()
				else:
					print(f'El contenedor {fila['Container']} no se encuentra en inventario')

	flash(f'El inventario se ha actualizado con los contenedores bloqueados.')
	return redirect(url_for('cargar'))		


#actualizanción Ofac Cuba
@app.route('/update_ofac', methods=['POST'])
def ofacc():
    if request.method == 'POST':
        archivo = request.files['archivo']
        if archivo and archivo.filename.endswith('.xls'):
            print("archivo aceptado")
            df = pd.read_excel(archivo)
            print('leyendo archivo')
                            
            excel_containersA = set(df['A'].astype(str).tolist())
            excel_containersB = set(df['B'].astype(str).tolist())
            excel_total = excel_containersA.union(excel_containersB)
            
            # Convertir a lista y dividir en lotes de 500
            excel_list = list(excel_total)
            batch_size = 500
            
            for i in range(0, len(excel_list), batch_size):
                batch = excel_list[i:i + batch_size]
                Contenedor.query.filter(Contenedor.containerNo.in_(batch)).update(
                    {'ofacc': 'Y'},
                    synchronize_session=False
                )
                print(f'Procesando lote {i//batch_size + 1}')

            db.session.commit()
            flash(f'OFAC actualizado: {len(excel_total)} contenedores marcados')
        return redirect(url_for('cargar'))


@app.route('/traslado', methods = ['POST'])
def traslado():
	if request.method == 'POST':
		archivo = request.files['archivo']
		if archivo and archivo.filename.endswith('.xls'):
			print('Archivo aceptado')
			df = pd.read_excel(archivo)
			print('leyendo archivo')

			for _,fila in df.iterrows():
				container = Contenedor.query.filter_by(containerNo = fila['Container']).first()
				if container:
					container.traslado = 'Y'
					db.session.commit()
					print(f'El contenedor {container.containerNo} actualizado a traslado')
			print(f'Actualización de contenedores de traslados completada')
			return redirect(url_for('cargar'))



if __name__ == '__main__':
	app.run(debug = True)