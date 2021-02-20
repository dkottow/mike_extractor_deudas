function="reduce_function"
aws_function_name="Mike_OCR_Reduce"
requeriments='xlsxwriter'

printf "Agregando librerías:\n\n"

cd ./${function}

mkdir ./packages

pip install ${requeriments} --target="./packages"

cd ./packages

printf "\nComprimiendo librerías:\n\n"


zip -r9 ${function}_deploy.zip ./

cd ..

mv ./packages/${function}_deploy.zip ./
rm -rf ./packages

printf "\nComprimiendo código de la función:\n\n"

zip -r9 -g ${function}_deploy.zip ./

cd ..

mv ${function}/${function}_deploy.zip ./

printf "\nTamaño del comprimido del deploy:\n\n"

ls -l --block-size=M | grep ${function}_deploy.zip

printf "\nSubiendo código a la función ${aws_function_name}\n\n"

aws lambda update-function-code --function-name ${aws_function_name} --zip-file fileb://${function}_deploy.zip