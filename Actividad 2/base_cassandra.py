""" Estudiante: Freddy Carrión
    Asignatura: Sistemas de Almacenamiento y Gestión Big Data
    Actividad 2
"""


# Importación de librerías necesarias para conexión con Cassandra y gestión de fechas
from cassandra.cluster import Cluster
from datetime import date


# Parte 1: Definición de clases de las entidades y relaciones
class Sucursal:
    def __init__(self, Nombre, Ciudad, Activo, id):  # Constructor de entidad Sucursal
        self.Nombre = Nombre
        self.Ciudad = Ciudad
        self.Activo = Activo
        self.id = id


class Cuenta:
    def __init__(self, Numero, Saldo, Servicios):  # Constructor con relación 1:n entre Sucursal y Cuenta
        self.Numero = Numero
        self.Saldo = Saldo
        self.Servicios = Servicios
        self.id = id


class CuentaBen:
    def __init__(self, Numero, DNIPasaporte):  # Constructor de relación n:m entre Cuenta y Beneficiario
        self.Numero = Numero
        self.DNIPasaporte = DNIPasaporte


class Beneficiario:
    def __init__(self, Nombre, DNIPasaporte):  # Constructor de entidad Beneficiario
        self.Nombre = Nombre
        self.DNIPasaporte = DNIPasaporte


class CuenTar:
    def __init__(self, Numero, Nombre, Limite):  # Constructor de relación n:m entre Cuenta y Tarjeta
        self.Numero = Numero
        self.Nombre = Nombre
        self.Limite = Limite


class Tarjeta:
    def __init__(self, Nombre, Tipo):  # Constructor de entidad Tarjeta
        self.Nombre = Nombre
        self.Tipo = Tipo


class CuenCli:
    def __init__(self, Numero, DNIPasaporte):  # Constructor de relación n:m entre Cuenta y Cliente
        self.Numero = Numero
        self.DNIPasaporte = DNIPasaporte


class Cliente:
    def __init__(self, DNIPasaporte, Nombre, Calle, Ciudad):  # Constructor de entidad Cliente
        self.DNIPasaporte = DNIPasaporte
        self.Nombre = Nombre
        self.Calle = Calle
        self.Ciudad = Ciudad


class CliPres:
    def __init__(self, DNIPasaporte, Numero):  # Constructor de relación n:m entre Cliente y Prestamo
        self.DNIPasaporte = DNIPasaporte
        self.Numero = Numero


class Prestamo:
    def __init__(self, Numero, Cantidad, id):  # Constructor con relación 1:n entre Sucursal y Prestamo
        self.Numero = Numero
        self.Cantidad = Cantidad
        self.id = id


# Parte 2: Creación de tablas soporte

