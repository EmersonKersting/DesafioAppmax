import requests
import csv
import re
from time import sleep

def clean_cnpj(cnpj):
    return re.sub(r'[^0-9]', '', cnpj)


def extract_activity_code(activity):
    if not activity or not isinstance(activity, dict) or 'code' not in activity:
        return ''

    code = activity['code']
    return re.sub(r'[^\d]', '', code)


def query_cnpj(cnpj):
    limpar_cnpj = clean_cnpj(cnpj)
    if not limpar_cnpj:
        return None

    url = f"https://www.receitaws.com.br/v1/cnpj/{limpar_cnpj}"

    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except requests.exceptions.RequestException as e:
        print(f"Erro ao consultar CNPJ {cnpj}: {e}")
        return None


def process_cnpj_data(cnpj_data):
    if not cnpj_data or 'status' in cnpj_data and cnpj_data['status'] == 'ERROR':
        return None

    atividade_principal = ''
    if 'atividade_principal' in cnpj_data and cnpj_data['atividade_principal']:
        atividade_principal = extract_activity_code(cnpj_data['atividade_principal'][0])

    atividade_secundaria = ''
    if 'atividades_secundarias' in cnpj_data and cnpj_data['atividades_secundarias']:
        atividade_secundaria = extract_activity_code(cnpj_data['atividades_secundarias'][0])

    return {
        'cnpj': clean_cnpj(cnpj_data.get('cnpj', '')),
        'data_abertura': cnpj_data.get('abertura', ''),
        'nome': cnpj_data.get('nome', ''),
        'atividade_principal': atividade_principal,
        'atividade_secundaria': atividade_secundaria,
        'ultima_atualizacao': cnpj_data.get('ultima_atualizacao', ''),
        'capital_social': cnpj_data.get('capital_social', '')
    }


def read_input_csv(input_file):
    cnpjs = []
    with open(input_file, mode='r', encoding='utf-8') as file:
        reader = csv.reader(file)
        for row in reader:
            if row:
                cnpjs.append(row[0])
    return cnpjs


def write_output_csv(output_file, data):
    fieldnames = [
        'CNPJ', 'data de abertura', 'nome', 'atividade_principal',
        'atividade_secundaria', 'ultima_atualizacao', 'capital_social'
    ]

    with open(output_file, mode='w', encoding='utf-8', newline='') as file:
        writer = csv.DictWriter(file, fieldnames=fieldnames, delimiter=';')
        writer.writeheader()
        for row in data:
            if row:
                writer.writerow({
                    'CNPJ': row['cnpj'],
                    'data de abertura': row['data_abertura'],
                    'nome': row['nome'],
                    'atividade_principal': row['atividade_principal'],
                    'atividade_secundaria': row['atividade_secundaria'],
                    'ultima_atualizacao': row['ultima_atualizacao'],
                    'capital_social': row['capital_social']
                })


def main(input_file, output_file):
    cnpjs = read_input_csv(input_file)
    enriched_data = []

    for cnpj in cnpjs:
        print(f"Processando CNPJ: {cnpj}")

        cnpj_data = query_cnpj(cnpj)

        if cnpj_data:
            processed_data = process_cnpj_data(cnpj_data)
            if processed_data:
                enriched_data.append(processed_data)

        sleep(20)

    write_output_csv(output_file, enriched_data)
    print(f"Processo concluído. Dados salvos em {output_file}")


if __name__ == "__main__":
    INPUT_FILE = "dados_iniciais.csv"
    OUTPUT_FILE = "dados_tratados.csv"

    main(INPUT_FILE, OUTPUT_FILE)