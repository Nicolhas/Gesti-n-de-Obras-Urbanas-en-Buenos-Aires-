from peewee import *

sqlite_db = SqliteDatabase('./obras_urbanas.db', pragmas={'journal_mode': 'wal'})

class BaseModel(Model):
    class Meta:
        database = sqlite_db

class Etapa(BaseModel):
    estado = CharField(max_length=25, unique=True)

    def __str__(self):
        return self.estado
    
    class Meta:
        db_table = 'etapa'

class TipoObra(BaseModel):
    tipo = CharField(max_length=40, unique=True)

    def __str__(self):
        return self.tipo
    
    class Meta:
        db_table = 'tipo_obra'

class AreaResponsable(BaseModel):
    area = CharField(max_length=60, unique=True)

    def __str__(self):
        return self.area
    
    class Meta:
        db_table = 'area_responsable'

class Comuna(BaseModel):
    numero = IntegerField(unique=True)

    def __str__(self):
        return self.numero
    
    class Meta:
        db_table = 'comuna'

class Barrio(BaseModel):
    nombre = CharField(max_length=25, unique=True)
    comuna = ForeignKeyField(Comuna, backref='comuna')

    def __str__(self):
        return f'{self.nombre}\n{self.comuna.numero}'
    
    class Meta:
        db_table = 'barrio'

class Empresa(BaseModel):
    nombre = CharField(max_length=80, unique=True)

    def __str__(self):
        return self.nombre
    
    class Meta:
        db_table = 'empresa'

class TipoContratacion(BaseModel):
    tipo = CharField(max_length=40, unique=True)

    def __str__(self):
        return self.tipo
    
    class Meta:
        db_table = 'tipo_contratacion'

class FuenteFinanciamiento(BaseModel):
    fuente = CharField(max_length=30, unique=True, null=True)

    def __str__(self):
        return self.fuente
    
    class Meta:
        db_table = 'fuente_financiamiento'

class Obra(BaseModel):
    nombre = CharField(max_length=100, null=True, unique=True)
    monto_contrato = FloatField(null=True)
    fecha_inicio = DateField(null=True)
    fecha_fin_inicial = DateField(null=True)
    plazo_meses = FloatField(null=True)
    porcentaje_avance = IntegerField(null=True)
    nro_contratacion = CharField(max_length=35, null=True)
    mano_obra = IntegerField(null=True)
    destacada = CharField(max_length=2, null=True)
    nro_expediente = CharField(max_length=50, null=True)

    etapa = ForeignKeyField(Etapa, backref='etapa')
    tipo_obra = ForeignKeyField(TipoObra, backref='tipo_obra')
    area_responsable = ForeignKeyField(AreaResponsable, backref='area_responsable')
    comuna = ForeignKeyField(Comuna, backref='comuna')
    barrio = ForeignKeyField(Barrio, backref='barrio', null=True)
    empresa = ForeignKeyField(Empresa, backref='empresa', null=True)
    tipo_contratacion = ForeignKeyField(TipoContratacion, backref='tipo_contratacion', null=True)
    fuente_financiamiento = ForeignKeyField(FuenteFinanciamiento, backref='fuente_financiamiento', null=True)

    def __str__(self):
        return f'{self.nombre}\n{self.etapa.estado}\n{self.tipo_obra.tipo}\n{self.area_responsable.area}\n{self.monto_contrato}\n{self.comuna.numero}\n{self.barrio.nombre}\n{self.fecha_inicio}\n{self.fecha_fin_inicial}\n{self.plazo_meses}\n{self.porcentaje_avance}\n{self.empresa.nombre}\n{self.tipo_contratacion.tipo}\n{self.nro_contratacion}\n{self.mano_obra}\n{self.destacada}\n{self.nro_expediente}\n{self.monto_contrato}\n{self.fuente_financiamiento.fuente}'
        
    class Meta:
        db_table = 'obra'

    # Metodos de clase
    def nuevo_proyecto():
        try:
            etapa, _ = Etapa.get_or_create(estado="Proyecto")
        except IntegrityError as e:
            print(f'Error al insertar el valor "Proyecto" dentro de la tabla etapa: {e}')

        nombreProyecto = input('Ingresar el nombre de obra que se va realizar: ')
        
        while True:
            tipo_obra = input('Ingresar el tipo de obra que se va realizar: ')

            try:
                tipoObra = TipoObra.get(TipoObra.tipo == tipo_obra)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({tipo_obra}) no existe en la base de datos\nPor favor ingrese un valor existente')

        while True:
            area_responsable = input('Ingresar el area responsable de la obra: ')

            try:
                areaResponsable = AreaResponsable.get(AreaResponsable.area == area_responsable)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({area_responsable}) no existe en la base de datos\nPor favor ingrese un valor existente')

        while True:
            barrio = input('Ingresar el barrio en el que se realizará la obra: ')

            try:
                bar = Barrio.get(Barrio.nombre == barrio)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({barrio}) no existe en la base de datos\nPor favor ingrese un valor existente')

        while True:
            comuna = int(input('Ingresar la comuna en la que se realizará la obra: '))

            try:
                com = Comuna.get(Comuna.numero == comuna)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({comuna}) no existe en la base de datos\nPor favor ingrese un valor existente')
        
        try:
            Obra.create(
                nombre=nombreProyecto,
                monto_contrato=None,
                fecha_inicio=None,
                fecha_fin_inicial=None,
                plazo_meses=None,
                porcentaje_avance=None,
                nro_contratacion=None,
                mano_obra=None,
                destacada=None,
                nro_expediente=None,
                etapa_id=etapa,
                tipo_obra_id=tipoObra,
                area_responsable_id=areaResponsable,
                comuna_id=com,
                barrio_id=bar,
                empresa_id=None,
                tipo_contratacion_id=None,
                fuente_financiamiento_id=None
                )
            
            print ("Nuevo proyecto creado con exito")
        except IntegrityError as e:
            print(f'No se pudo cargar el pryecto: {e}')
        
    def iniciar_contratacion():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        while True:
            tipo_contratacion = input('Ingresar el tipo de contratación: ')

            try:
                tipoContratacion = TipoContratacion.get(TipoContratacion.tipo == tipo_contratacion)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({tipo_contratacion}) no existe en la base de datos\nPor favor ingrese un valor existente')

        nro_contratacion = input('Ingresar el número de contratación: ')
        monto_contrato = float(input('Ingresar el monto del contrato: '))

        proyecto.tipo_contratacion = tipoContratacion
        proyecto.nro_contratacion = nro_contratacion
        proyecto.monto_contrato = monto_contrato

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")
        
    def adjudicar_obra():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        while True:
            empresa = input('Ingresar el nombre de la empresa: ')

            try:
                emp = Empresa.get(Empresa.nombre == empresa)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({empresa}) no existe en la base de datos\nPor favor ingrese un valor existente')

        nro_expediente = input('Ingresar el número de expediente: ')

        proyecto.empresa = emp
        proyecto.nro_expediente = nro_expediente

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")

    def iniciar_obra():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        while True:
            destacada = input('Indicar si la obra se categoriza como destacada (SI/NO): ')
            if destacada == 'SI' or destacada == 'NO':
                break
            else:
                print('La respuesta no es valida.\nIngresar la respuesta nuevamente')

        fecha_inicio = input('Indicar la fecha en la que se iniciará la obra: ')
        fecha_fin_inicial = input('Indicar la fecha en la que se estima finalizar la obra: ')

        while True:
            fuente_financiamiento = input('Ingresar la fuiente de financiamiento: ')

            try:
                fuente = FuenteFinanciamiento.get(FuenteFinanciamiento.fuente == fuente_financiamiento)
                break
            except DoesNotExist as e:
                print(f'El valor ingresado ({fuente_financiamiento}) no existe en la base de datos\nPor favor ingrese un valor existente')

        mano_obra = int(input('Indicar la cantidad de mano de obra: '))

        proyecto.destacada = destacada
        proyecto.fecha_inicio = fecha_inicio
        proyecto.fecha_fin_inicial = fecha_fin_inicial
        proyecto.fuente_financiamiento = fuente
        proyecto.mano_obra = mano_obra

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")


    def actualizar_porcentaje_avance():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        nuevo_avance = int(input('Indicar el porcentaje de avance de la obra: '))

        proyecto.porcentaje_avance = nuevo_avance

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")


    def incrementar_plazo():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        plazo_meses = int(input('Indicar la cantidad de meses que lleva la obra en ejecución: '))

        proyecto.plazo_meses = plazo_meses

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")


    def incrementar_mano_obra():
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        nueva_mano_obra = int(input('Ingresar cuanta mano de obra se incorporará: '))

        proyecto.mano_obra += nueva_mano_obra

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")


    def finalizar_obra():
        
        etapa = Etapa.get(Etapa.estado == "Finalizada")
        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        proyecto.porcentaje_avance = 100
        proyecto.etapa = etapa
        
        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")
        
        
    def rescindir_obra():

        etapa, _ = Etapa.get_or_create(estado="Rescindida")

        proyecto = Obra.get(Obra.etapa == Etapa.get(Etapa.estado == 'Proyecto'))

        proyecto.etapa = etapa

        try:
            proyecto.save() 
            print("Se ha guardado con éxito")
        except IntegrityError as e:
            print(f"Error al actualizar los datos {e}")