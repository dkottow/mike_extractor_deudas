# MikeExtractorDeudas_02_PDF2Descriptor

Función lambda que convierte un pdf a un descriptor de imagen.
Es la segunda dentro del pipeline de Mike.

## Especificaciones:

Esta función requiere un ambiente basado en **`Python 3.7`** que posea:

- boto3
- numpy
- pdf2image
- opencv-python
- moto3 (http://docs.getmoto.org/en/latest/ , solo para testear)
- pytest (solo para testear)

Además, la función debe contar con la siguiente(s) políticas de ejecución en AWS:

- Mike_S3

## Configuración de la función en AWS

La configuración necesaria en la página de AWS Lambda para esta función es:

- 1024 ram, 
- 5 min timeout
- Layers: numpy, opencv_python_headless, poppler.

Además, debe tener un desencadenador/trigger desde S3 que cumpla la siguiente especificación

    Event type: ObjectCreated
    Prefix: Intermedio/Map/
    Suffix: .pdf

## Tests

Los tests vienen por defecto configurados para ejecutarse usando mocks (servicios simulados locales) de AWS. 
Para testear el funcionamiento de la infraestructura de esta funcion en la nube, puedes el valor de la variable
`MOCK_TESTS` del archivo `src/config.py` asignando a esta `False`.

Para ejecutar los tests (ya teniendo instalados `pytest` y `moto`), en un terminal posicionado en la ruta raiz del
repositorio, ejecutar: 

```bash
python -m pytest 
```


## Instrucciones para hacer un deploy manual

1. Hacer deploy de un layer de poppler siguiendo la siguiente guía :

https://www.petewilcock.com/using-poppler-pdftotext-and-other-custom-binaries-on-aws-lambda/

2. Hacer el deploy usando github actions

Cada vez que se haga un push a `main`, se ejecutaran los linters, los test (si es que existen) y el auto-deploy.

Para hacer deploy manual, ejecutar:

```
bash deploy.bash
```

Nota: Para hacer el deploy manual es importante tener el CLI de AWS instalado y sus credenciales credenciales configuradas.

Para generar solo el build, ejecutar:

```
bash deploy.bash --only-build
```

o en forma abreviada:

```
bash deploy.bash --ob
```

## Adicionales:

- El código está formateado a 120 de ancho usando `Black`
- El código fue desarrollado usando los linters pylint, flake8 y mypy

