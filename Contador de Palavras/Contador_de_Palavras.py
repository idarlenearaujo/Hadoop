import glob
import csv
import mincemeat

# lista de todos os nomes de arquivos dentro da pasta selecionada
arquivos = glob.glob('textos\\*')

# abre o arquivo em questão e retorna seu conteudo a variável source e o fecha por fim
def ler_arquivos(file_name):

    texto = open(file_name)

    try:
        return texto.read()
    finally:
        texto.close()

# cria um dicionário (KEY, VALUE)
source = dict((file_name, ler_arquivos(file_name))for file_name in arquivos)

# MAP
# Indicado um MAP para cada bloco
# Um worker que possui uma tarefa de map lê o conteúdo correspondente ao pedaço da entrada.
# Ele interpreta os pares chave/valor a partir dos dados de entrada e passa como parâmetro para a função de map.
# Os pares chave/valor intermediários produzidos pela função de map são armazenados em memória.
def mapfn(k, v):
    from stopwords import allStopWords
    for phrase in v.splitlines():
        for word in phrase.split():
            if word not in allStopWords:
                yield word, 1

# REDUCE
# A localização desses pares de dados no disco é informada ao master, que irá repassar essa localização para os workers com tarefas de reduce.
# O worker de reduce percorrer os dados intermediários já ordenados e para cada chave encontrada,
# ele passa a chave os valores intermediários para a função de reduce definida pelo usuário.
def reducefn(k, v):
    return sum(v)

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

# A saída de cada função de reduce é adicionada ao final de um arquivo de saída para aquela partição de reduce.
result = s.run_server(password='changeme')

w = csv.writer(open('resultado.csv', 'w'))

for k, v in result.items():
    w.writerow([k, v])
