import os
from os import path
import json
import pandas as pd
import itertools
#from tqdm.notebook import tqdm

from tqdm import tqdm


# Nombre de los archivos json en la carpeta de trabajo
path_to_json = 'contratacionesabiertas_bulk_paquetes_json/'
json_files = [pos_json for pos_json in os.listdir(path_to_json) if pos_json.endswith('.json')]

# Revisa que exista la carpeta donde se ponen los json procesados
path_to_processed_json = 'processed/'
is_exist = os.path.exists(path_to_processed_json)

if not is_exist:

   # Crea el directorio si no existe
   os.makedirs(path_to_processed_json)
   print(f"Se creo el directorio: {path_to_processed_json}")
else:
   print(f"El directorio \"{path_to_processed_json}\" ya existe")

# Get data columns name
# sample_size = 10
# df_sample = pd.read_csv('ecda_processed_26022023.csv',nrows=sample_size, low_memory=False)
# cols_main = list(df_sample.columns)


# Nombre de los archivos json en la carpeta de trabajo ya procesados
json_files_processed = [
    pos_json for pos_json in os.listdir(path_to_processed_json) if pos_json.endswith('.json')
    ]

from os import path

# ------------- recursive processing

for json_name_test in tqdm(json_files_processed):

    #if (path.exists('./csv/'+json_name_test[:-5]+'.csv')) or (json_name_test == 'ext_contratacionesabiertas_bulk_paquete5.json'):
    if (path.exists('./csv/zip/'+json_name_test[:-5]+'.zip')):

        print(f"File {json_name_test} already processed --- Skipping")

    else:
        # Lee el contenido del archivo json como diccionatio
        with open(os.path.join(path_to_processed_json, json_name_test)) as json_file:

            data = json.load(json_file)
            print(f"Subiendo archivo {json_name_test} :",len(data), "registros")

            temp_data = pd.DataFrame(data)

            # for x in cols_main:

            #     if x not in temp_data.columns:

            #         temp_data[x] = ''

            # temp_data= temp_data[cols_main]

            temp_data.to_csv('./csv/'+json_name_test[:-5]+'.csv')