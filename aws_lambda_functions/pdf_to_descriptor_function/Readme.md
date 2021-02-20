# Instrucciónes Función Lambda PDF_TO_DESCRIPTOR

1. Hacer deploy de un layer de poppler siguiendo la siguiente guía :

https://www.petewilcock.com/using-poppler-pdftotext-and-other-custom-binaries-on-aws-lambda/

2. Crear si es que no existe la función en AWS lambda.

3. Hacer deploy de la función usando el script `deploy_pdf_to_descriptor.bash`.

Debes estar parado en la carpeta `lambda_functions` (o en la carpeta padre de la función) para generar el package y
hacer el deploy.

Ejemplo:

```bash
[ec2-user@ip-172-31-31-46 lambda_functions]$ bash deploy_pdf_to_descriptor.bash
```

El script cuenta con tres variables, que puede que deban ser cambiadas:

```bash
# Nombre de la carpeta en donde se encuentra la función, suponiendo que estas parado en la ruta padre de esta.
# También debería servir la ruta completa... (pero no esta probado)
function="pdf_to_descriptor_function"
# Nombre de la función en AWS lambda
aws_function_name="Mike_OCR_PDF_To_Descriptor"
# Requerimientos por instalar usando pip
requeriments='pdf2image'
```

El proceso del deploy en breves palabras es:

- Instalar las dependencias usando pip en la carpeta .NOMBRE_FUNCION/packages
- Comprimir la carpeta de las dependencias en el archiv deploy.zip.
- Borrar la carpeta .NOMBRE_FUNCION/packages.
- Comprimir dentro del mismo deploy.zip la carpeta .NOMBRE_FUNCION
- Hacer el deploy usando el CLI de AWS.

4. En la consola de la función lambda, agregar las siguientes variables de entorno:

```bash
LD_LIBRARY_PATH=/lib64:/usr/lib64:$LAMBDA_RUNTIME_DIR:$LAMBDA_RUNTIME_DIR/lib:$LAMBDA_TASK_ROOT:$LAMBDA_TASK_ROOT/lib:/opt/lib

PATH=/usr/local/bin:/usr/bin/:/bin:/opt/bin
```
