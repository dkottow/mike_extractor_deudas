# Documentación OCR:

Requerimientos:

- numpy
- pandas
- scikit-learn
- opencv-python
- boto3
- pdf2image

## Estructura del módulo:

El módulo OCR está diseñado para que todos los archivos sean procesados por un pipeline.
Todos los pasos del pipeline se encuentran definidos en métodos dentro de la misma clase. La mayoría de estas funciones llaman a clases definidas en archivos externos.

El pipeline para forum es una clase que se encuentra en el archivo `forum_pipeline.py`

El pipeline se ejecuta con el método asíncrono `execute_detection` y se compone de los siguientes pasos:

1. Cargar el archivo desde el bucket

Este paso ejecuta el método `load_file(bucket, path)`, llama a boto3 para obtener el archivo desde s3 y retorna un stream de bytes con el archivo solicitado.

2. Clasificar el tipo de documento

`classify_file(document)`: Toma el stream de bytes del paso anterior, convierte el stream (suponiendo que es un pdf) a imagen y luego lo clasifica usando el clasificador `forum_doc_clf`.
Retorna el tipo del documento.

3. Escoger el tipo de procesador usado y el tipo de analisis que Textextract ejecutará

En este paso, dado el tipo de documento, se determina que procesador (es decir, la clase con regex y otros) que se utilizará para extraer la información.
También se determina el tipo de detección que TextExtract ejecutará sobre ese archivo (OCR, tablas formulario).

4. Analizar el archivo

En esta etapa, por medio del método `analyze_file(role_arn, path, process_type, filename)`, se hace la llamada asíncrona a TextExtract. Se pone en pausa la cantidad de tiempo que se especifique en el archivo de configuración.

5. Parsear resultados

En este paso se parsean los resultados obtenidos por TextExtract.
De esto se encarga la función `parse_response`, la cual retorna una tupla (text, tables, forms)

6. Analizar respuesta y retornar

Usa el procesador seleccionado en el paso 3 para parsear los textos, tablas y formularios entregados en la etapa pasada.
Este paso llama al método `process_response(procesador, texto, tablas, forms, filename)`

Una vez finalizada la detección, retorna un diccionario con la siguiente estructura:

```python
{
    'filename': '',
    'document_type': '',
    'data': ''
}
```

`data` contiene las columnas extraidas desde cada archivo, más el nombre el archivo y los errores de detección.

## Cómo ejecutar un pipeline:

El siguiente código muestra un ejemplo de cómo ejecutar el pipeline de la detección sobre un archivo en S3.

Nota: Para ejecutar un método asíncrono en python, anteponer `await`

```python
pipe = DocumentPipeline()
results = await pipe.execute_detection(role_arn, bucket, path)
results
```

## Como ejecutar asíncronamente varios pipes:

Esta parte aún es experimental.

Nota: Cuando no se está en jupyter, es necesario ejecutar

```python
asyncio.run()
```

para ejecutar la corutinas.
Jupyter por defecto ejecuta esto, por lo que no es necesario.

```python

import asyncio
from ocr.forum_pipeline import DocumentPipeline


# file_list = Lista con los archivos por procesar desde s3

pipelines  = [(DocumentPipeline(), path) for path in file_list]

tasks = [pipeline.execute_detection(role_arn, bucket, path) for pipeline, path in pipelines]

results = await asyncio.gather(*tasks)


```

En el código

- `pipelines` contiene los pipelines creados para cada archivo. Una vez terminada la ejecución, se puede inspeccionar uno a uno los resultados de cada archivo

DEV:

https://2lb2v05bnd.execute-api.us-east-2.amazonaws.com/dev
