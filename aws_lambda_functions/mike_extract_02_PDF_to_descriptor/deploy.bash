AWS_FUNCTION_NAME="mike-extract-02-pdf_to_descriptor"
AWS_REGION="sa-east-1"

REQUIREMENTS_TO_IGNORE='boto3|numpy|pandas|scikit-learn|matplotlib|opencv-python'
ONLY_BUILD=0

# Buscar argumentos de entrada
for arg in "$@"
do
    case $arg in
        -ob|--only-build)
        ONLY_BUILD=1
        shift # Remove --only-build from processing
        ;;
    esac
done

printf "\n________________________________________________________________\n"
printf "\nEjecutando Build y deploy de la función $AWS_FUNCTION_NAME"
printf "\n________________________________________________________________\n"

if [ -d ./build ];
then
printf "\nEliminando build anterior...\n"
rm -rf ./build
fi

printf "\nAgregando librerías (ignorando ${REQUIREMENTS_TO_IGNORE}):\n"
mkdir ./build


if [ -f requirements.txt ]
    then
    requirements=$(grep -ivE "${REQUIREMENTS_TO_IGNORE}" requirements.txt)
    if [ -n "$requirements" ]
        then

        printf "\nInstalando librerías en ./build/:\n\n"
        if [ "$(type -t pip)" = 'alias' ]; then
            pip install ${requirements} --target="./build/"
        else
            pip3 install ${requirements} --target="./build/"
        fi        

        else
            printf "No se encontró ninguna librería agregable al build...\n"
    fi
fi
printf "\nComprimiendo librerías y el código de la función:\n"

cp -r ./src/ ./build
cp ./lambda_function.py ./build

cd ./build
zip -r9 ./build.zip ./

printf "\nInformación del build:\n"
ls -l --block-size=M | grep build.zip


# Deploy solo en el caso que sea solicitado
if [ "$ONLY_BUILD" = 0 ];
then
    printf "\nSubiendo código a la función ${AWS_FUNCTION_NAME}\n"
    aws lambda update-function-code --function-name ${AWS_FUNCTION_NAME} --zip-file fileb://build.zip --region ${AWS_REGION}

else
    printf "\nSe omitió el deploy a AWS"
fi

printf "\nOk!\n"
cd ..