create database  prueba_practica_ar_holdings;
use prueba_practica_ar_holdings;

CREATE TABLE catalogo_articulos (
ID VARCHAR(50) NOT NULL,
SKU VARCHAR(50) NOT NULL,
Imagenurl VARCHAR(50) NOT NULL,
Nombre VARCHAR(50) NOT NULL,
Cantidad INTEGER NOT NULL,
FechaRegistro DATE NOT NULL,
UltimaFechaActualizacion DATE NOT NULL,
Sincronizado BOOLEAN NOT NULL,
PRIMARY KEY (ID)); 

CREATE TABLE catalogo_Logarticulos (
ID VARCHAR(50) NOT NULL,
IDArticulo VARCHAR(50) NOT NULL,
Sjson VARCHAR(50) NOT NULL,
FechaRegistro DATE NOT NULL,
PRIMARY KEY (ID),
foreign key(IDArticulo) references catalogo_articulos(ID)); 

create table facturacion_encabezado (
ID VARCHAR(50) NOT NULL,
NumeroOrtden VARCHAR(50) NOT NULL,
Total INT NOT NULL,
Fecha DATE NOT NULL,
Moneda VARCHAR(50) NOT NULL,
PRIMARY KEY (ID));

create table facturacion_cliente (
ID VARCHAR(50) NOT NULL,
IDEncabezado VARCHAR(50) NOT NULL,
Nombre VARCHAR(50) NOT NULL,
Telefono VARCHAR(50) NOT NULL,
Correo VARCHAR(50) NOT NULL,
Direccion VARCHAR(50) NOT NULL,
PRIMARY KEY (ID),
foreign key (IDEncabezado) references facturacion_encabezado(ID));

create table facturacion_detalle (
ID VARCHAR(50) NOT NULL,
IDEncabezado VARCHAR(50) NOT NULL,
Imagenurl VARCHAR(50) NOT NULL,
SKU VARCHAR(50) NOT NULL,
Nombre VARCHAR(50) NOT NULL,
Cantidad INT NOT NULL,
Precio INT NOT NULL,
Total INT NOT NULL,
PRIMARY KEY (ID),
foreign key (IDEncabezado) references facturacion_encabezado(ID));

DELETE FROM catalogo_articulos