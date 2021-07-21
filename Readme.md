
# Mike Extractor de Deudas

---
## CloudFormation

Pasos para hacer el deploy usando CloudFormation: 

### 1. Preparar todo antes de crear los builds

- Primero, instalar `zip` (en el caso de no tenerlo):

```bash 
sudo apt install .zip
```

- Luego, instalar la CLI de AWS: https://aws.amazon.com/es/cli/
y después configurar las credenciales: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html

- Después, crear un bucket llamado `mike-resources-prod` (el cual contendrá las funciones y las layers). Esto se puede hacer  en la consola de S3 (https://s3.console.aws.amazon.com/s3/) como también usando el siguiente script:

```bash
if aws s3 ls "s3://mike-resources-prod-1" 2>&1 | grep -q 'NoSuchBucket'
then
printf "Creando bucket..."
aws s3api create-bucket --bucket mike-resources-prod-1 --region sa-east-1 --create-bucket-configuration LocationConstraint=sa-east-1
else
printf "El Bucket ya existe.\n"
fi
```

- Por último, activar un ambiente de **`Python 3.7`**. usando pipenv/conda/etc...

### 2. Generar los builds: 

Ejecutar:

```bash
bash build.bash
```

Este comando hará un build de todas las funciones y sus dependencias. 
Usará la versión de pip que esté activada cuando se ejecute el comando. Por este motivo, es importante que sea compatible con el runtime de mike, que es por defecto python 3.7.


### 3. En el caso que todo salga bien, el build recién generado está en la carpeta `builds`. 

Ahora, se debe copiar los archivos generados por el build a el bucket de los recursos. Para esto:

Copiar el contenido del 

```bash
build/build_mas_actual/functions 
```

a la siguiente ruta en el bucket `mike-resources-prod`: 

```bash
mike-resources-prod/ExtractorDeudas/LambdaFunctions/
```

y el contenido de 
```bash
build/build_mas_actual/layers 
```

a la siguiente ruta en el bucket `mike-resources-prod`: 

```bash
mike-resources-prod/ExtractorDeudas/LambdaLayers/
```

### 4. Cloudformation...

