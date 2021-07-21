
BUILD_DIR="./builds/$(date +%H_%M__%d_%m_%Y)"

mkdir -p $BUILD_DIR/layers
mkdir -p $BUILD_DIR/functions

# -------------------------------------------------------------------
# Funciones

# moverse a la carpeta de los lambdas.
cd aws_lambda_functions

# crear el arreglo con las carpetas con las funciones lambda.
readarray -d '' LAMBDAS < <(find .  -mindepth 1 -maxdepth 1 -type d)


echo "Directorios encontrados: ${LAMBDAS}"

for LAMBDA in $LAMBDAS
do
    echo "Procesando ${LAMBDA}"
    cd $LAMBDA

    # borrar el build anterior (si es que existe)
    rm -rf ./build

    # generar el build
    bash ./deploy.bash --only-build

    # mover el build a la carpeta build en la raiz.
    mv ./build/build.zip ../../$BUILD_DIR/functions/$LAMBDA.zip

    # borrar la build
    rm -rf ./build

    # retornar a la carpeta raiz de las lambdas.
    cd ..
done

cd ..

# -------------------------------------------------------------------
# Layers

printf "\n\nCopiando Layers\n"

cd ./aws_lambda_layers
readarray -d '' LAYERS < <(find .  -mindepth 1 -maxdepth 2  | grep .zip)
for LAYER in $LAYERS
do 
    cp -f $LAYER ../$BUILD_DIR/layers/$(basename -- $LAYER)
done

printf "\nListo!!!\n"
