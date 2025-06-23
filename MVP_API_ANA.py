import requests
import xmltodict
import csv
import os
import pandas as pd
from urllib.parse import urlencode
from datetime import datetime,date
import calendar 
import certifi
#obtendo o primeiro e último dia do mês atual
hoje = date.today()

primeiro_dia_mes = hoje.replace(day=1)
ultimo_dia_mes = hoje.replace(day=calendar.monthrange(hoje.year, hoje.month)[1])

# Obtendo o token de acesso para requisitar os dados da API principal
url_token = 'https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/OAUth/v1'
headers = {
    "accept": "*/*",
    "identificador": "07939296000150",
    "senha": "5g9wpg_g"
}
requisicao = requests.get(url_token, headers=headers)
if requisicao.status_code == 200:
    print("Requisição de token bem-sucedida")
    resposta_json = requisicao.json()
    token_autenticacao = resposta_json["items"]["tokenautenticacao"]
    print("Token recebido:", token_autenticacao)
else:
    print("Erro na requisição de token")

# Configurando as estações e parâmetros para a API principal
codigos_estacoes_pluvio = [
    "2043005", "2043013", "2044007", "2044033", "2044065", "2044080", "2044103",
    "1944004", "1944007", "1944026", "1944027", "1944055", "1944059", "1944062",
    "2044008", "2044016", "2044019", "2044020", "2044021", "2044024", "2044026",
    "2044041", "2044043", "2044052", "2044053", "2044054", "2044102", "2044081",
    "1944083", "1944010", "1944031", "1944049", "1944084", "1844027", "1844028"
]

base_dados_pluvio = "dados_pluvio"
for codigo_pluvio in codigos_estacoes_pluvio:
    Params_pluvio = {
        "Código da Estação": codigo_pluvio,
        "Tipo Filtro Data": "DATA_LEITURA",
        "Data Inicial (yyyy-MM-dd)": primeiro_dia_mes.strftime('%Y-%m-%d'),
        "Data Final (yyyy-MM-dd)": ultimo_dia_mes.strftime('%Y-%m-%d')
    }
    base_url_pluvio = 'https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieChuva/v1/?'
    url_pluvio = base_url_pluvio + urlencode(Params_pluvio)

    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {token_autenticacao}"
    }

    requisicao_pluvio = requests.get(url_pluvio, headers=headers)

    if requisicao_pluvio.status_code == 200:
        resposta_json = requisicao_pluvio.json()
        message = resposta_json.get("message", "").lower()

        if message != "sucesso":
            print(f"Mensagem retornada: {message}. Tentando API alternativa para o código {codigo_pluvio}.")

            data_inicio = primeiro_dia_mes.strftime('%Y/%m/%d')
            data_fim = ultimo_dia_mes.strftime('%Y/%m/%d')
            estacoes_soap = [codigo_pluvio]
            data_obj = datetime.strptime(data_inicio, "%Y/%m/%d")
            ano = str(data_obj.year)
            mes = str(data_obj.month).zfill(2)

            for estacao in estacoes_soap:
                body = f"""<?xml version="1.0" encoding="utf-8"?>
                <soap:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xmlns:xsd="http://www.w3.org/2001/XMLSchema"
                xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">
                <soap:Body>
                    <HidroSerieHistorica xmlns="http://MRCS/">
                        <codEstacao>{codigo_pluvio}</codEstacao>
                        <dataInicio>{data_inicio}</dataInicio>
                        <dataFim>{data_fim}</dataFim>
                        <tipoDados>2</tipoDados>
                        <nivelConsistencia>1</nivelConsistencia>
                    </HidroSerieHistorica>
                </soap:Body>
                </soap:Envelope>"""

                url_soap = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx"
                headers_soap = {
                    "Content-Type": "text/xml; charset=utf-8",
                    "SOAPAction": "http://MRCS/HidroSerieHistorica"
                }

                response = requests.post(url_soap, data=body, headers=headers_soap)

                if response.status_code == 200:
                    print("Requisição SOAP bem-sucedida")
                    data_dict = xmltodict.parse(response.content)

                    try:
                        resultado_raw = data_dict['soap:Envelope']['soap:Body']['HidroSerieHistoricaResponse']['HidroSerieHistoricaResult']
                        resultado_dict = resultado_raw 
                        series = resultado_dict['diffgr:diffgram']['DocumentElement']['SerieHistorica']

                        caminho = "dados_pluvio"
                        os.makedirs(caminho, exist_ok=True)

                        filename = os.path.join(caminho, f"{codigo_pluvio}.csv")

                        campos = [
                            'EstacaoCodigo', 'NivelConsistencia', 'DataHora', 'TipoMedicaoChuvas', 'Maxima',
                            'Total', 'DiaMaxima', 'NumDiasDeChuva', 'MaximaStatus', 'TotalStatus', 'NumDiasDeChuvaStatus',
                            'TotalAnual', 'TotalAnualStatus', 
                            'Chuva01', 'Chuva02', 'Chuva03', 'Chuva04', 'Chuva05', 'Chuva06', 'Chuva07', 'Chuva08', 'Chuva09', 'Chuva10',
                            'Chuva11', 'Chuva12', 'Chuva13', 'Chuva14', 'Chuva15', 'Chuva16', 'Chuva17', 'Chuva18', 'Chuva19', 'Chuva20',
                            'Chuva21', 'Chuva22', 'Chuva23', 'Chuva24', 'Chuva25', 'Chuva26', 'Chuva27', 'Chuva28', 'Chuva29', 'Chuva30', 'Chuva31',
                            'Chuva01Status', 'Chuva02Status', 'Chuva03Status', 'Chuva04Status', 'Chuva05Status', 'Chuva06Status', 'Chuva07Status',
                            'Chuva08Status', 'Chuva09Status', 'Chuva10Status', 'Chuva11Status', 'Chuva12Status', 'Chuva13Status', 'Chuva14Status',
                            'Chuva15Status', 'Chuva16Status', 'Chuva17Status', 'Chuva18Status', 'Chuva19Status', 'Chuva20Status', 'Chuva21Status',
                            'Chuva22Status', 'Chuva23Status', 'Chuva24Status', 'Chuva25Status', 'Chuva26Status', 'Chuva27Status', 'Chuva28Status',
                            'Chuva29Status', 'Chuva30Status', 'Chuva31Status', 'DataIns'
                        ] 
                        with open(filename, 'w', newline='', encoding='utf-8') as f:
                            writer = csv.DictWriter(f, fieldnames=campos)
                            writer.writeheader()
                            for item in series:
                                linha = {campo: item.get(campo, "") for campo in campos}
                                writer.writerow(linha)

                        print(f"Arquivo salvo em {filename}")

                    except Exception as e:
                        print("Erro ao processar dados SOAP:", e)
                        print("Sem dados para essa estação")
                else:
                    print("Erro na requisição SOAP:", response.status_code)
                    print(response.text)

        else:
            print("Requisição bem-sucedida no código:", codigo_pluvio)
            dados = resposta_json["items"]
            nome_arquivo_csv_pluvio = os.path.join(base_dados_pluvio, f"pluviometria_{codigo_pluvio}.csv")
            df = pd.DataFrame(dados)
            df.to_csv(nome_arquivo_csv_pluvio, mode='a', index=False, header=not os.path.exists(nome_arquivo_csv_pluvio), encoding='utf-8')
    else:
        print(requisicao_pluvio.status_code)
        print("Erro na requisição de pluviometria")

# PROCESSO DE REQUEST FLUVIO
codigo_estacoes_fluvio = [
    "40549998", "40579995", "40680000", "40810800", "40811100", "40823500", "40850000",
    "40710000", "40740000", "40800001", "40799000", "40865400", "40866000", "40680000", "40799000"
]
base_dados_fluvio = "dados_fluvio"

for codigo_fluvio in codigo_estacoes_fluvio:
    Params_fluvio = {
        "Código da Estação": codigo_fluvio,
        "Tipo Filtro Data": "DATA_LEITURA",
        "Data Inicial (yyyy-MM-dd)": "2024-01-01",
        "Data Final (yyyy-MM-dd)": "2024-12-01"
    }
    base_url_fluvio = 'https://www.ana.gov.br/hidrowebservice/EstacoesTelemetricas/HidroSerieVazao/v1?'
    url_fluvio = base_url_fluvio + urlencode(Params_fluvio)

    headers = {
        "accept": "*/*",
        "Authorization": f"Bearer {token_autenticacao}"
    }

    requisicao_fluvio = requests.get(url_fluvio, headers=headers)

    if requisicao_fluvio.status_code == 200:
        resposta_json = requisicao_fluvio.json()
        message = resposta_json.get("message", "").lower()

        if message != "sucesso":
            print(f"Mensagem retornada: {message}. Tentando API alternativa para o código {codigo_fluvio}.")

            data_inicio = primeiro_dia_mes.strftime('%Y/%m/%d')
            data_fim = ultimo_dia_mes.strftime('%Y/%m/%d')
            estacoes_soap_fluvio = [codigo_fluvio]
            data_obj = datetime.strptime(data_inicio, "%Y/%m/%d")
            ano = str(data_obj.year)
            mes = str(data_obj.month).zfill(2)

            for estacao_fluvio in estacoes_soap_fluvio:
                body = f"""<?xml version="1.0" encoding="utf-8"?>
                <soap12:Envelope xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance" xmlns:xsd="http://www.w3.org/2001/XMLSchema" xmlns:soap12="http://www.w3.org/2003/05/soap-envelope">
                  <soap12:Body>
                    <DadosHidrometeorologicos xmlns="http://MRCS/">
                      <codEstacao>{estacao_fluvio}</codEstacao>
                      <dataInicio>{data_inicio}</dataInicio>
                      <dataFim>{data_fim}</dataFim>
                    </DadosHidrometeorologicos>
                  </soap12:Body>
                </soap12:Envelope>"""

                url_soap_fluvio = "http://telemetriaws1.ana.gov.br/ServiceANA.asmx"
                headers_soap_fluvio = {
                    "Content-Type": "application/soap+xml; charset=utf-8"
                }

                response_soap_fluvio = requests.post(url_soap_fluvio, headers=headers_soap_fluvio, data=body)

                if response_soap_fluvio.status_code == 200:
                    data_dict_fluvio = xmltodict.parse(response_soap_fluvio.content)

                    try:
                        series_historicas_fluvio = data_dict_fluvio['soap:Envelope']['soap:Body']['DadosHidrometeorologicosResponse']['DadosHidrometeorologicosResult']['diffgr:diffgram']['DocumentElement']['DadosHidrometereologicos']

                        caminho_fluvio = base_dados_fluvio
                        os.makedirs(caminho_fluvio, exist_ok=True)

                        filename = os.path.join(caminho_fluvio, f"fluviometria_{ano}{mes}_{estacao_fluvio}.csv")
                        fields = ['CodEstacao', 'DataHora', 'Vazao', 'Nivel', 'Chuva']

                        with open(filename, 'w', newline='') as csvfile:
                            writer = csv.DictWriter(csvfile, fieldnames=fields)
                            writer.writeheader()
                            for serie_historica_fluvio in series_historicas_fluvio:
                                writer.writerow({
                                    'CodEstacao': serie_historica_fluvio['CodEstacao'],
                                    'DataHora': serie_historica_fluvio['DataHora'],
                                    'Vazao': serie_historica_fluvio['Vazao'],
                                    'Nivel': serie_historica_fluvio['Nivel'],
                                    'Chuva': serie_historica_fluvio['Chuva']
                                })

                        print(f"Os dados da estação {estacao_fluvio} no mês {mes} e ano {ano} foram exportados para o arquivo '{filename}'.")

                    except KeyError:
                        print(f"Não há dados para a estação {estacao_fluvio} no mês {mes} e no ano {ano}.")
                else:
                    print(f"Erro na requisição da API SOAP para a estação {estacao_fluvio}.")
        else:
            print("Requisição bem-sucedida no código:", codigo_fluvio)
            dados = resposta_json.get("items", [])
            if dados:
                nome_arquivo_csv_fluvio = os.path.join(base_dados_fluvio, f"fluviometria_{codigo_fluvio}.csv")
                df = pd.DataFrame(dados)
                df.to_csv(nome_arquivo_csv_fluvio, mode='a', index=False, header=not os.path.exists(nome_arquivo_csv_fluvio), encoding='utf-8')
            else:
                print(f"Sem dados para a estação {codigo_fluvio}.")
    else:
        print(requisicao_fluvio.status_code)
        print("Erro na requisição de fluviometria.")