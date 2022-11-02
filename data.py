import requests
from datetime import datetime
import pandas as pd
import os
import shutil

### teste####

def clear_folder(path):
    '''
    Função que deleta todos os arquivos dentro de uma pasta.

    path: caminho relativo da pasta que vai ser limpada.
    '''
    files = [path + f for f in os.listdir(path)]

    for arquivo in files:

        try:

            shutil.rmtree(arquivo)

        except:

            os.remove(arquivo)

def create_folders():
    '''
    Função que cria a estrutura de pasta para os dados desse projeto.
    '''

    if not os.path.exists('data_lake'):

        os.makedirs('data_lake')

    if not os.path.exists('data_lake/raw'):

        os.makedirs('data_lake/raw')

    if not os.path.exists('data_lake/trusted'):

        os.makedirs('data_lake/trusted')

def download():
    '''
    Função que realiza o download da tabela de dados mais recente do site!

    OBS: Atualmente está sendo utilizado o CSV completo nesse processo, pois a API estava com bastante instabilidade.
    '''

    print('# Baixando os dados\n')

    url = 'https://data.brasil.io/dataset/covid19/caso_full.csv.gz'

    now = datetime.now().strftime("%Y%m%d_%H_%M")

    r = requests.get(url)

    if r.status_code == 200:

        with open(f'data_lake/raw/caso_full_{now}.csv.gz', 'wb') as f:

            f.write(r.content)

            print('Download dos dados realizado com sucesso!\n')

    else:

        print('Falha ao fazer o download dos dados!\n')

def transform():
    '''
    Realiza as transformações e limpezas de dados necessárias para que os dados se tornem confiáveis.
    '''

    print('# Transformando os dados\n')

    # Procurando o nome do arquivo mais recente

    files = ['data_lake/raw/' + f for f in os.listdir('data_lake/raw/')]
    
    latest_file = max(files, key = os.path.getctime)

    # Lendo a base de dados

    df = pd.read_csv(latest_file, compression='gzip')

    # Selecionando somente registros de cidades

    df = df.loc[df['place_type'] == 'city']  = df.loc[df['place_type'] == 'city'] 

    # Tratando registros sem o código do cidade 
    # Para esses casos é criado um ID único para o estado e colocado ele.

    aux_state_id = df['state'].drop_duplicates().reset_index(drop = True)
    aux_state_id = pd.Series(aux_state_id.index, index = aux_state_id.values).to_dict()

    df.loc[df['city_ibge_code'].isna(), 'city_ibge_code'] = df.loc[df['city_ibge_code'].isna(), 'state'].map(aux_state_id)

    # Selecionando somente colunas necessárias

    cols = ['city_ibge_code', 'date', 'city', 'state', 'estimated_population', 'order_for_place', 'new_confirmed', 'new_deaths']

    df = df[cols]

    # Renomeando colunas

    map_rename_columns = {
        'city_ibge_code' : 'ID_LOCAL',
        'date' : 'DT_REGISTRO',
        'city' : 'NM_CIDADE',
        'state' : 'NM_ESTADO',
        'estimated_population' : 'VL_POPULACAO_ESTIMADA',
        'order_for_place' : 'COD_SEQUENCIA_POR_LOCAL',
        'new_confirmed' : 'VL_CONFIRMADOS',
        'new_deaths' : 'VL_MORTOS'
    }

    df.rename(columns = map_rename_columns, inplace = True)

    # Garantindo Formatação dos dados

    map_type_data = {
        'ID_LOCAL' : 'Int64',
        'DT_REGISTRO' : 'datetime64[ns]',
        'NM_CIDADE' : 'object',
        'NM_ESTADO' : 'object',
        'VL_POPULACAO_ESTIMADA' : 'Int64',
        'COD_SEQUENCIA_POR_LOCAL' : 'Int64',
        'VL_CONFIRMADOS' : 'Int64',
        'VL_MORTOS' : 'Int64'
    }

    for var in map_type_data.keys():

        if map_type_data[var] == 'datetime64[ns]':

            df[var] = pd.to_datetime(df[var], errors = 'coerce')

        else:
            
            df[var] = df[var].astype(map_type_data[var])

    # Limpando a pasta

    clear_folder(path = 'data_lake/trusted/')

    # Salvando base de dados

    now = datetime.now().strftime("%Y%m%d_%H_%M")

    df.to_parquet(f'data_lake/trusted/caso_full_{now}.parquet.gzip', compression='gzip', engine='fastparquet', index = False)

if __name__ == "__main__":

    create_folders()
    download()
    transform()