"""
Rutas de la aplicación CCIS.
"""

import pandas as pd
from flask import Blueprint, render_template, request, redirect, url_for, flash, Response
from .models import Contenedor
from . import db
import os
import csv
import io

# Obtener la ruta base del proyecto
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

main_bp = Blueprint('main', __name__, template_folder=os.path.join(BASE_DIR, 'templates'))


@main_bp.route('/')
def home():
    """Página principal con lista de contenedores activos."""
    contenedores = Contenedor.query.filter_by(active='Y').all()
    return render_template('home.html', containers=contenedores)


@main_bp.route('/filtro')
def filtro():
    """Filtrar contenedores según criterios seleccionados."""
    # Obtener arrays de valores EXCLUIDOS (los desmarcados)
    isos_excluidos = request.args.getlist('iso')
    grados_excluidos = request.args.getlist('grado')
    statuses_excluidos = request.args.getlist('status')
    solo_ofac = request.args.get('ofac')

    # Construir consulta base
    query = Contenedor.query
    
    # Aplicar filtros de EXCLUSIÓN (NOT IN)
    if isos_excluidos:
        query = query.filter(~Contenedor.iso.in_(isos_excluidos))
    if grados_excluidos:
        query = query.filter(~Contenedor.grado.in_(grados_excluidos))
    if statuses_excluidos:
        query = query.filter(~Contenedor.status.in_(statuses_excluidos))
    
    # Filtro OFAC
    if solo_ofac:
        query = query.filter(Contenedor.ofacc == 'Y')

    # Ejecutar consulta
    contenedores_filtrados = query.all()
    
    return render_template('filtro.html', containers=contenedores_filtrados)


@main_bp.route('/cargar')
def cargar():
    """Página para carga de archivos."""
    return render_template('carga.html')


@main_bp.route('/generar')
def generar():
    """Generar lista completa de contenedores con paginación y opción de exportación."""
    # Obtener parámetros de paginación y ordenamiento
    page = request.args.get('page', 1, type=int)
    per_page = request.args.get('per_page', 50, type=int)
    sort_by = request.args.get('sort', 'containerNo')
    order = request.args.get('order', 'asc')
    
    # Validar campos de ordenamiento permitidos
    allowed_sort_fields = ['containerNo', 'iso', 'grado', 'status', 'days', 'ofacc', 'block', 'traslado']
    if sort_by not in allowed_sort_fields:
        sort_by = 'containerNo'
    
    # Validar dirección de ordenamiento
    if order not in ['asc', 'desc']:
        order = 'asc'
    
    # Obtener columna de ordenamiento
    sort_column = getattr(Contenedor, sort_by, None)
    if sort_column is None:
        sort_column = Contenedor.containerNo
    
    # Aplicar ordenamiento
    if order == 'desc':
        sort_column = sort_column.desc()
    
    # Ejecutar consulta con paginación y ordenamiento
    pagination = Contenedor.query.order_by(sort_column).paginate(
        page=page, 
        per_page=per_page, 
        error_out=False
    )
    containers = pagination.items
    
    return render_template('datos.html', 
                         containers=containers, 
                         pagination=pagination,
                         current_sort=sort_by,
                         current_order=order,
                         per_page=per_page)


@main_bp.route('/generar/export')
def generar_export():
    """Exportar lista completa de contenedores a CSV."""
    # Obtener todos los contenedores con ordenamiento
    sort_by = request.args.get('sort', 'containerNo')
    order = request.args.get('order', 'asc')
    
    allowed_sort_fields = ['containerNo', 'iso', 'grado', 'status', 'days', 'ofacc', 'block', 'traslado']
    if sort_by not in allowed_sort_fields:
        sort_by = 'containerNo'
    
    sort_column = getattr(Contenedor, sort_by, None)
    if sort_column is None:
        sort_column = Contenedor.containerNo
    
    if order == 'desc':
        sort_column = sort_column.desc()
    
    containers = Contenedor.query.order_by(sort_column).all()
    
    # Crear archivo CSV en memoria
    output = io.StringIO()
    writer = csv.writer(output)
    
    # Escribir encabezados
    writer.writerow(['Contenedor', 'ISO', 'Grade', 'Status', 'Days', 'OFAC', 'Block', 'Traslado', 'Remarks'])
    
    # Escribir datos
    for c in containers:
        writer.writerow([
            c.containerNo,
            c.iso,
            c.grado,
            c.status,
            c.days,
            c.ofacc,
            c.block,
            c.traslado,
            c.remark
        ])
    
    output.seek(0)
    
    # Crear respuesta para descargar archivo
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={
            'Content-Disposition': f'attachment; filename=contenedores_{sort_by}_{order}.csv'
        }
    )


@main_bp.route('/upload_inventario', methods=['POST'])
def upload_inventario():
    """Actualizar o crear inventario desde archivo Excel."""
    if request.method == 'POST':
        container_db = Contenedor.query.all()
        archivo = request.files['archivo']
        
        if not archivo or not archivo.filename.endswith('.xls'):
            flash('El formato del archivo no es válido, solo se permite archivos xls.', 'warning')
            return redirect(url_for('main.cargar'))
        
        try:
            if container_db:
                # Actualización de inventario existente
                print("Actualizando inventario")
                df = pd.read_excel(archivo)
                
                for _, fila in df.iterrows():
                    exist = Contenedor.query.filter_by(containerNo=fila['Container No']).first()
                    if exist:
                        print('Contenedor en inventario')
                    else:
                        new_container = Contenedor(
                            containerNo=fila['Container No'],
                            iso=fila['ISO'],
                            grado=fila['Grade'],
                            status=fila['Sts'],
                            days=fila['Days'],
                            remark=fila['Remarks']
                        )
                        print(f'Agregando nuevo contenedor {new_container.containerNo}')
                        db.session.add(new_container)
                        db.session.commit()

                # Actualización de contenedores - desactivar los que no están en el Excel
                excel_container = set(df['Container No'].astype(str).tolist())
                db_actual = {container.containerNo for container in container_db}

                desactivate = db_actual - excel_container
                Contenedor.query.filter(
                    Contenedor.containerNo.in_(desactivate)
                ).update(
                    {'active': 'N'},
                    synchronize_session=False
                )
                db.session.commit()
                print(f'Se desactivaron los contenedores que ya no están en la base de datos {len(desactivate)}')

                print('Actualización Finalizada')
                flash('Actualización de inventario completada', 'success')
            else:
                # Creación inicial de base de datos
                print("Creando base de datos.")
                print('leyendo archivo')
                df = pd.read_excel(archivo)
                print(df)
                
                for _, fila in df.iterrows():
                    print(fila)
                    nuevo_contenedor = Contenedor(
                        containerNo=fila['Container No'],
                        iso=fila['ISO'],
                        grado=fila['Grade'],
                        status=fila['Sts'],
                        days=fila['Days'],
                        remark=fila['Remarks']
                    )
                    print(f'Agregando contenedor {nuevo_contenedor.containerNo}')
                    db.session.add(nuevo_contenedor)
                    db.session.commit()
                
                flash('Datos cargados con éxito', 'success')

        except Exception as e:
            print(e)
            flash(f'Error al realizar la carga masiva: {str(e)}', 'danger')

        return redirect(url_for('main.cargar'))
    
    return redirect(url_for('main.cargar'))


@main_bp.route('/update_blok', methods=['POST'])
def update_blok():
    """Actualizar estado de bloqueados desde archivo Excel."""
    if request.method == 'POST':
        archivo = request.files['archivo']
        
        if archivo and archivo.filename.endswith('.xls'):
            print("archivo aceptado")
            df = pd.read_excel(archivo)
            print("leyendo archivo")
            
            for _, fila in df.iterrows():
                print(f'Buscando contenedor {fila["Container"]}')
                container = Contenedor.query.filter_by(containerNo=fila['Container']).first()
                if container:
                    print(f'El Contenedor {container.containerNo} será actualizado a estatus bloqueado')
                    container.block = 'Y'
                    db.session.commit()
                else:
                    print(f'El contenedor {fila["Container"]} no se encuentra en inventario')

        flash('El inventario se ha actualizado con los contenedores bloqueados.', 'success')
        return redirect(url_for('cargar'))
    
    return redirect(url_for('cargar'))


@main_bp.route('/update_ofac', methods=['POST'])
def update_ofac():
    """Actualizar estado OFAC desde archivo Excel."""
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
                Contenedor.query.filter(
                    Contenedor.containerNo.in_(batch)
                ).update(
                    {'ofacc': 'Y'},
                    synchronize_session=False
                )
                print(f'Procesando lote {i//batch_size + 1}')

            db.session.commit()
            flash(f'OFAC actualizado: {len(excel_total)} contenedores marcados', 'success')
        
        return redirect(url_for('cargar'))
    
    return redirect(url_for('cargar'))


@main_bp.route('/traslado', methods=['POST'])
def traslado():
    """Actualizar estado de traslado desde archivo Excel."""
    if request.method == 'POST':
        archivo = request.files['archivo']
        
        if archivo and archivo.filename.endswith('.xls'):
            print('Archivo aceptado')
            df = pd.read_excel(archivo)
            print('leyendo archivo')

            for _, fila in df.iterrows():
                container = Contenedor.query.filter_by(containerNo=fila['Container']).first()
                if container:
                    container.traslado = 'Y'
                    db.session.commit()
                    print(f'El contenedor {container.containerNo} actualizado a traslado')
            
            print(f'Actualización de contenedores de traslados completada')
            return redirect(url_for('cargar'))
    
    return redirect(url_for('cargar'))
