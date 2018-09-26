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
    total = 0
    for ks, vs in enumerate(v):
        if vs.split(':')[0] == 'Vendas':
            # total de venda por filial
            total = int(vs.split(':')[1]) + total
        if vs.split(':')[0] == 'Filial':
            # nome filial
            NomeFilial = vs.split(':')[1]

    # lista com resultados
    L = list()
    L.append(NomeFilial + ' , ' + str(total))
    return L

# Server
s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

# result
result = s.run_server(password='changeme')

w = csv.writer(open('result2.csv', 'w'))

for k, v in result.items():
    w.writerow([k, str(v).replace('[', '').replace(']', '').replace("'", '').replace(' ', '')])
