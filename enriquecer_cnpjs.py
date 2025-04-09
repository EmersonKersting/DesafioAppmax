import requests
import csv
import re
import sqlite3
from time import sleep

def clean_cnpj(cnpj):
    return re.sub(r'\D', '', cnpj)

def extract_activity_code(activity):
    return re.sub(r'\D', '', activity['code']) if isinstance(activity, dict) and 'code' in activity else ''

def query_cnpj(cnpj):
    cnpj = clean_cnpj(cnpj)
    if not cnpj:
        return None
    try:
        resp = requests.get(f"https://www.receitaws.com.br/v1/cnpj/{cnpj}")
        resp.raise_for_status()
        return resp.json()
    except requests.RequestException as e:
        print(f"Erro ao consultar {cnpj}: {e}")
        return None

def process_cnpj_data(data):
    if not data or data.get('status') == 'ERROR':
        return None
    return {
        'cnpj': clean_cnpj(data.get('cnpj', '')),
        'data_abertura': data.get('abertura', ''),
        'nome': data.get('nome', ''),
        'atividade_principal': extract_activity_code(data['atividade_principal'][0]) if data.get('atividade_principal') else '',
        'atividade_secundaria': extract_activity_code(data['atividades_secundarias'][0]) if data.get('atividades_secundarias') else '',
        'ultima_atualizacao': data.get('ultima_atualizacao', ''),
        'capital_social': data.get('capital_social', '')
    }

def read_input_csv(path):
    with open(path, 'r', encoding='utf-8') as f:
        return [row[0] for row in csv.reader(f) if row]

def write_output_csv(path, data):
    campos = ['CNPJ', 'data de abertura', 'nome', 'atividade_principal', 'atividade_secundaria', 'ultima_atualizacao', 'capital_social']
    with open(path, 'w', encoding='utf-8', newline='') as f:
        writer = csv.DictWriter(f, fieldnames=campos, delimiter=';')
        writer.writeheader()
        for row in data:
            writer.writerow({
                'CNPJ': row['cnpj'],
                'data de abertura': row['data_abertura'],
                'nome': row['nome'],
                'atividade_principal': row['atividade_principal'],
                'atividade_secundaria': row['atividade_secundaria'],
                'ultima_atualizacao': row['ultima_atualizacao'],
                'capital_social': row['capital_social']
            })

def classificar_datas(caminho_csv, saida_csv):
    conn = sqlite3.connect(':memory:')
    cur = conn.cursor()

    with open(caminho_csv, 'r', encoding='utf-8') as f:
        reader = csv.reader(f, delimiter=';')
        headers = next(reader)
        headers_sql = [h.replace(" ", "_") for h in headers]
        cur.execute(f'CREATE TABLE empresas ({", ".join(f"{h} TEXT" for h in headers_sql)})')

        for linha in reader:
            cur.execute(f'INSERT INTO empresas VALUES ({",".join("?" for _ in headers_sql)})', linha)
    conn.commit()

    cur.execute("""SELECT e.*, freq.contagem AS frequencia,
       RANK() OVER (ORDER BY freq.contagem DESC) AS classificacao
    FROM empresas e
    JOIN (
    SELECT data_de_abertura, COUNT() AS contagem
    FROM empresas
    GROUP BY data_de_abertura
) freq ON e.data_de_abertura = freq.data_de_abertura
    ORDER BY freq.contagem DESC
""")

    with open(saida_csv, 'w', encoding='utf-8', newline='') as f:
        writer = csv.writer(f, delimiter=';')
        writer.writerow([col[0] for col in cur.description])
        writer.writerows(cur.fetchall())

    print(f"\n📊 Classificação por datas salva em: {saida_csv}")
    conn.close()

def main(input_file, output_file):
    cnpjs = read_input_csv(input_file)
    dados = []

    for cnpj in cnpjs:
        print(f"Consultando: {cnpj}")
        info = query_cnpj(cnpj)
        if info:
            processado = process_cnpj_data(info)
            if processado:
                dados.append(processado)
        sleep(20)

    write_output_csv(output_file, dados)
    print(f"\n✅ CSV salvo em: {output_file}")

    classificar_datas(output_file, "dados_classificados.csv")

if __name__ == "__main__":
    main("dados_iniciais.csv", "dados_tratados.csv")
