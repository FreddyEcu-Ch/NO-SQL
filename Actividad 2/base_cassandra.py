""" Estudiante: Freddy Carrión
    Asignatura: Sistemas de Almacenamiento y Gestión Big Data
    Actividad 2
"""


# Importación de librerías necesarias para conexión con Cassandra y gestión de fechas
from cassandra.cluster import Cluster


# Parte 1: Definición de clases de las entidades y relaciones
class Sucursal:
    def __init__(self, Sucursal_id, Sucursal_Nombre, Sucursal_Ciudad, Sucursal_Activo):  # Constructor de entidad Sucursal
        self.Sucursal_Nombre = Sucursal_Nombre
        self.Sucursal_Ciudad = Sucursal_Ciudad
        self.Sucursal_Activo = Sucursal_Activo
        self.Sucursal_id = Sucursal_id


class Cuenta:
    def __init__(self, Cuenta_Numero, Cuenta_Saldo, Cuenta_Servicios, Sucursal_id):  # Constructor con relación 1:n entre Sucursal y Cuenta
        self.Cuenta_Numero = Cuenta_Numero
        self.Cuenta_Saldo = Cuenta_Saldo
        self.Cuenta_Servicios = Cuenta_Servicios
        self.Sucursal_id = Sucursal_id


class CuentaBen:
    def __init__(self, Cuenta_Numero, Beneficiario_DNIPasaporte):  # Constructor de relación n:m entre Cuenta y Beneficiario
        self.Cuenta_Numero = Cuenta_Numero
        self.Beneficiario_DNIPasaporte = Beneficiario_DNIPasaporte


class Beneficiario:
    def __init__(self, Beneficiario_Nombre, Beneficiario_DNIPasaporte):  # Constructor de entidad Beneficiario
        self.Beneficiario_Nombre = Beneficiario_Nombre
        self.Beneficiario_DNIPasaporte = Beneficiario_DNIPasaporte


class CuenTar:
    def __init__(self, Cuenta_Numero, Tarjeta_Nombre, Limite):  # Constructor de relación n:m entre Cuenta y Tarjeta
        self.Cuenta_Numero = Cuenta_Numero
        self.Tarjeta_Nombre = Tarjeta_Nombre
        self.Limite = Limite


class Tarjeta:
    def __init__(self, Tarjeta_Nombre, Tarjeta_Tipo):  # Constructor de entidad Tarjeta
        self.Tarjeta_Nombre = Tarjeta_Nombre
        self.Tarjeta_Tipo = Tarjeta_Tipo


class CuenCli:
    def __init__(self, Cuenta_Numero, Cliente_DNIPasaporte):  # Constructor de relación n:m entre Cuenta y Cliente
        self.Cuenta_Numero = Cuenta_Numero
        self.Cliente_DNIPasaporte = Cliente_DNIPasaporte


class Cliente:
    def __init__(self, Cliente_DNIPasaporte, Cliente_Nombre, Cliente_Calle, Cliente_Ciudad):  # Constructor de entidad Cliente
        self.Cliente_DNIPasaporte = Cliente_DNIPasaporte
        self.Cliente_Nombre = Cliente_Nombre
        self.Cliente_Calle = Cliente_Calle
        self.Cliente_Ciudad = Cliente_Ciudad


class CliPres:
    def __init__(self, Cliente_DNIPasaporte, Prestamo_Numero):  # Constructor de relación n:m entre Cliente y Prestamo
        self.DNIPasaporte = Cliente_DNIPasaporte
        self.Prestamo_Numero = Prestamo_Numero


class Prestamo:
    def __init__(self, Prestamo_Numero, Prestamo_Cantidad, Sucursal_id):  # Constructor con relación 1:n entre Sucursal y Prestamo
        self.Prestamo_Numero = Prestamo_Numero
        self.Prestamo_Cantidad = Prestamo_Cantidad
        self.Sucursal_id = Sucursal_id


# Parte 2: Creación de tablas soporte
def SoporteBeneficiario(DNIPasaporte):
    select = session.prepare("SELECT * FROM cliente_por_dni WHERE id = ?")  # solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute (select, [DNIPasaporte, ])   # Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas:
        c = Cliente(DNIPasaporte, fila.nombre, fila.calle,  fila.ciudad)  # creamos instancia del cliente
        return c


def SoporteCuenta(Numero):
    select = session.prepare("SELECT * FROM cuenta_por_numero WHERE numero = ?")  # solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute(select, [Numero, ])   # Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas:
        cu = Cuenta(Numero, fila.saldo, fila.servicios, fila.id)  # creamos instancia de la cuenta
        return cu


def SoporteSucursal(id):
    select = session.prepare("SELECT * FROM sucursal_por_id WHERE id = ?")  # solo va a devolver una filia pero lo tratamos como si fuesen varias
    filas = session.execute(select, [id, ])   # Importante, aunque solo haya un valor a reemplazar en el preparedstatemente, hay que poner la ','
    for fila in filas:
        s = Cuenta(id, fila.nombre, fila.ciudad, fila.activo)  # creamos instancia de las sucursales
        return s


# Parte 3: Métodos para inserción de datos
def insertCuenta(): # Método para insertar datos en entidad Cuenta
    #Pedimos al usuario del programa los datos de la cuenta bancaria
    numero = int(input("Dame el numero de cuenta"))
    saldo = int(input("Dame el saldo actual"))
    id = input("Dame el id de la sucursal")
    servicios = set()  # iniciamos la colección (set) que contendrá los servicios a insertar
    servicio = input("Introduzca una servicio, vacío para parar")
    while servicio != "":
        servicios.add(servicio)
        servicio = input("Introduzca un servicio, vacío para parar")

    cu = Cuenta(numero, saldo, servicios, id)
    # Insertar datos en tabla SoporteCuenta
    insertStatementSo = session.prepare("INSERT INTO cuenta_por_numero(numero, saldo, servicios, id) VALUES (?, ?, ?, ?)")
    session.execute(insertStatementSo, [cu.Cuenta_Numero, cu.Cuenta_Saldo, cu.Cuenta_Servicios, cu.Sucursal_id])


def insertSucursal():  # Método para insertar datos en entidad Sucursal
    #Pedimos al usuario del programa los datos de la sucursal
    nombre = int(input("Dame el nombre de la sucursal"))
    id = input("Dame el id de la sucursal")
    ciudad = input("Dame el nombre de la ciudad del activo")
    activo = bool(input("Introduzca el activo"))
    su = Sucursal(id, nombre, ciudad, activo)

    # Insertar datos en tabla SoporteSucursal
    insertStatementSoSu = session.prepare("INSERT INTO sucursal_por_id(Sucursal_id, Sucursal_Nombre, Sucursal_Ciudad, Sucursal_Activo) VALUES (?, ?, ?, ?)")
    session.execute(insertStatementSoSu, [su.Sucursal_id, su.Sucursal_Nombre, su.Sucursal_Activo, su.Sucursal_Activo])


def insertBeneficiario():  # Método para insertar datos en entidad Beneficiario
    #Pedimos al usuario del programa los datos de la sucursal
    nombreBe = input("Dame el nombre del beneficiario")
    nombreCli = input("Dame el nombre del cliente")
    dniBe = input("Dame el dni")
    dniCli = input("Dame el dni del cliente")
    calle = input("Dame el nombre de la calle")
    ciudad = input("Dame el nombre de la ciudad")
    b = Beneficiario(nombreBe, dniBe)
    cli = Cliente(dniCli, nombreCli, calle, ciudad)

    # Insertar datos en tabla SoporteBeneficiario
    insertStatementSoBe = session.prepare("INSERT INTO cliente_por_dni(Cliente_DNI, Cliente_Nombre, Cliente_Calle, Cliente_Ciudad) VALUES (?, ?, ?, ?)")
    session.execute(insertStatementSoBe, [cli.Cliente_DNIPasaporte, cli.Cliente_Nombre, cli.Cliente_Calle, cli.Cliente_Ciudad])


def insertClienteCuentaSucursal():  # Método para insertar datos en consulta 3
    # Pedimos al usuario del programa los datos del cliente, cuenta con sus sucursales asociadas
    dni = input("Dame el dni del cliente")
    numero = int(input ("Dame el número de cuenta"))
    id = input("Dame la id de la sucursal")
    saldo = int(input("Dame el saldo de la cuenta"))
    nombre = input("Dame el nombre del cliente")
    nombreSu = input("Dame el nombre de la sucursal")
    ciudad = input ("Dame el nombre de la ciudad de la sucursal")

    # Insertar instancias para la consulta 3
    insertStatementCliCuSu = session.prepare("INSERT INTO tabla3(Cliente_DNI, Cuenta_Numero, Sucursal_id, Cuenta_Saldo, Cliente_Nombre, Sucursal_Ciudad, Sucursal_Nombre) VALUES (?, ?, ?, ?, ?, ?, ?)")
    session.execute(insertStatementCliCuSu, [dni, numero, nombre, id, saldo, nombre, ciudad, nombreSu])


def insertCuentaClienteBeneficiario():
    # Pedimos al usuario del programa los datos de un número de cuenta para obtener su información de clientes y beneficiarios
    numero = int(input("Dame el numero de cuenta"))
    dniBe = input("Dame el dni del beneficiario")
    dniCli = input("Dame el dni del cliente")
    nombreBe = input("Dame el nombre del beneficiario")
    nombre_Cli = input("Dame el nombre del cliente")
    saldo = int(input("Dame el saldo de la cuenta"))

    # Insertar instancias para la consulta 2
    insertStatementCuCliBe = session.prepare("INSERT INTO tabla2(Cuenta_Numero, Beneficiario_DNI, Cliente_DNI, Beneficiario_Nombre, Cliente_Nombre, Cuenta_Saldo) VALUES (?, ?, ?, ?)")
    session.execute(insertStatementCuCliBe, [numero, dniBe, dniCli, nombreBe, nombre_Cli, saldo])


def insertBeneficiarioCuentaSucursal():
    # Pedimos al usuario del programa los datos de un número de cuenta para obtener su información de clientes y beneficiarios
    nombre = input("Dame el nombre de la sucursal")
    numero = int(input("Dame el numero de cuenta"))
    dni = input("Dame el dni del beneficiario")
    nombreBe = input("Dame el nombre del beneficiario")
    saldo = int(input("Dame el saldo de la cuenta"))

    # Insertar instancias para la consulta 5
    insertStatementBeCuSu = session.prepare("INSERT INTO tabla5(Sucursal_Nombre, Cuenta_Numero, Beneficiario_DNI, Beneficiario_Nombre, Saldo_Cuenta) VALUES (?, ?, ?, ?, ?)")
    session.execute(insertStatementBeCuSu, [nombre, numero, dni, nombreBe, saldo])


# Inserción en relación Cuente Cliente (Depositante)
def insertDepositante():
    # Pedimos al usuario del programa los datos de un número de cuenta para obtener su información de clientes
    numero = int(input("Dame el numero de la cuenta"))
    dni = input("Dame el dni del cliente")
    depo = CuenCli(numero, dni)

    # Insertar instancias para la relación depositante
    insertStatementDe = session.prepare("INSERT INTO depositante(Cuenta_Numero, Cliente_DNI) VALUES (?, ?)")
    session.execute(insertStatementDe, [depo.Cuenta_Numero, depo.Cliente_DNIPasaporte])


# Inserción en relación Cuente Tarjeta (DetalleTar)
def insertDetalleTar():
    # Pedimos al usuario del programa los datos de un número de cuenta para información de las tarjetas asociadas
    numero = int(input("Dame el numero de la cuenta"))
    nombre = input("Dame el nombre de la tarjeta")
    limite = float(input("Dame el límite de la tarjeta"))
    tar = CuenTar(numero, nombre, limite)

    # Insertar instancias para la relación depositante
    insertStatementar = session.prepare("INSERT INTO detalletar(Cuenta_Numero, Tarjeta_Nombre, Limite) VALUES (?, ?, ?)")
    session.execute(insertStatementar, [tar.Cuenta_Numero, tar.Tarjeta_Nombre, tar.Limite])


# Inserción en relación Cuente Beneficiario (CuBen)
def insertCuBen():
    # Pedimos al usuario del programa los datos de un número de cuenta para información de las tarjetas asociadas
    numero = int(input("Dame el numero de la cuenta"))
    dni = input("Dame el dni del beneficiario")
    cuben = CuentaBen(numero, dni)

    # Insertar instancias para la relación depositante
    insertStatementCuBen = session.prepare("INSERT INTO cuben(Cuenta_Numero, Beneficiario_DNI) VALUES (?, ?)")
    session.execute(insertStatementCuBen, [cuben.Cuenta_Numero, cuben.Beneficiario_DNIPasaporte])


# Parte 4: Actualización de datos

# Parte 5: Consultas
def consultaSucursalPorId():
    id = input("Dame id de la sucursal")
    sucursal = SoporteSucursal(id)
    if sucursal != None: #si la sucursal no existe no mostramos nada
        print("id: ", sucursal.Sucursal_id)
        print("Nombre: ", sucursal)
        print("Direccion: ", sucursal)


def consultaClientePorDNI():
    dni = input("Ingrese el dni")
    cliente = SoporteBeneficiario(dni)
    if cliente != None: # Si el cliente no existe no mostramos nada
        print("DNI: ", cliente.Cliente_DNIPasaporte)
        print("Nombre: ", cliente.Cliente_Nombre)
        print("Ciudad: ", cliente.Cliente_Ciudad)
        print("Calle: ", cliente.Cliente_Calle)


def consultaCuentaPorNumero():
    numero = int(input("Ingrese el número de cuenta"))
    cuenta = SoporteCuenta(numero)
    if cuenta != None: # Si el cliente no existe no mostramos nada
        print("Número: ", cuenta.Cuenta_Numero)
        print("Saldo: ", cuenta.Cuenta_Saldo)
        print("Servicios: ", cuenta.Cuenta_Servicios)
        print("ID Sucursal: ", cuenta.Sucursal_id)


# Programa principal
# Conexión con Cassandra
cluster = Cluster()
cluster = Cluster(['127.0.0.1'], port=9042)
session = cluster.connect('freddycarrion')
numero = -1


# Sigue pidiendo operaciones hasta que se introduzca 0
while numero != 0:
    print("Introduzca un número para ejecutar una de las siguientes operaciones:")
    print("1. Insertar una Cuenta")
    print("2. Insertar una Sucursal")
    print("3. Insertar un Beneficiario")
    print("4. Insertar relación entre cliente, cuenta y sucursal")
    print("5. Insertar relación entre cuenta, cliente y beneficiario")
    print("6. Insertar relación entre beneficiario, cuenta y sucursal")
    print("7. Insertar relación entre cuenta y tarjeta (DetalleTar)")
    print("8. Insertar relación entre cuenta y cliente (depositante)")
    print("9. Insertar relación entre cuenta y beneficiario (CuBen)")
    print("10. Consultar datos de un cliente según su dni")
    print("11. Consultar datos de una sucursal según su id")
    print("12. Consultar datos de una cuenta según su número")
    #print ("8. Actualizar precio producto")
    print("0. Cerrar aplicación")

    numero = int(input())  # Pedimos numero al usuario
    if numero == 1:
        insertCuenta()
    elif numero == 2:
        insertSucursal()
    elif numero == 3:
        insertBeneficiario()
    elif numero == 4:
        insertClienteCuentaSucursal()
    elif numero == 5:
        insertCuentaClienteBeneficiario()
    elif numero == 6:
        insertBeneficiarioCuentaSucursal()
    elif numero == 7:
        insertDetalleTar()
    elif numero == 8:
        insertDepositante()
    elif numero == 9:
        insertCuBen()
    elif numero == 10:
        consultaClientePorDNI()
    elif numero == 11:
        consultaSucursalPorId()
    elif numero == 12:
        consultaCuentaPorNumero()
    else:
        print("Número incorrecto")
cluster.shutdown()  # cerramos conexión

