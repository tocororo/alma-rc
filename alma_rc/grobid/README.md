# Implementacion del cliente de grobid y la extraccion de metadatos.

El objetivo de este modulo es crear un script para extraer metadatos de ficheros pdf con el objetivo de poder incluir mas informacion en los records a insertar en earchivo.

## Implementacion actual

Por el momento existen cuatro ficheros:

1- **extract_metadata.py**: el punto de entrada del script a ejecutar

2- **grobid_client**: el encargado de conectarse al servicio de grobid en docker y computar el xml

3- **tei_parser.py**: extrae los metadatos del xml generado

4- **docker-compose.yml** que levanta el servidor de grobid

## TODO

1- Comprobar si es posible que procese PDF sin descargarlo del ftp

2- Integrarlo con el script de ftp_collector

3- Para procesar con grobid se utiliza el endpoint 'processHeaderDocument', puesto que es para metadatos generales (titulo, autor principalmente), pero es posible considerar otros, [consultar la documentacion](https://grobid.readthedocs.io/en/latest/Grobid-service/)