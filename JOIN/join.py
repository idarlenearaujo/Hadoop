import glob
import mincemeat
import csv

# lista com os nomes dos arquivos contidos na pasta
arquivos = glob.glob('2.2 Join\\*')

# abrir os arquivos e retornar seu conteúdo
def ler_arquivos(file_name):

    texto = open(file_name)

    try:
        return texto.read()
    finally:
        texto.close()

# dicionário contendo chave/valor
source = dict((file_name, ler_arquivos(file_name))for file_name in arquivos)

# MAP
def mapfn(k, v):
    for line in v.splitlines():
        if k == '2.2 Join\\2.2-vendas.csv':
            yield line.split(';')[0], 'Vendas' + ':' + line.split(';')[5]
        if k == '2.2 Join\\2.2-filiais.csv':
            yield line.split(';')[0], 'Filial' + ':' + line.split(';')[1]

# REDUCE
def reducefn(k, v):
    return v

# Server
s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

# result
result = s.run_server(password='changeme')

w = csv.writer(open('result.csv', 'w'))

for k, v in result.items():
    w.writerow([k, v])

