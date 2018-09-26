import glob
import mincemeat
import csv
import nltk
nltk.download()
from nltk.corpus import stopwords
sw = stopwords.words("english")

# arquivos
files = glob.glob('Trab2.3\\*')

# ler textos
def ler_arquivos(file_name):
    arquivo = open(file_name, encoding='utf8')

    try:
        return arquivo.read()
    finally:
        arquivo.close()

# chave / valor inicial
source = dict((file_name, ler_arquivos(file_name)) for file_name in files)

# MAP
# Name node especifica workers para executar a função MAP
# criando nova chave e valor ordenados para serem processados pela função reduce
def mapfn(k, v):
    for line in v.splitlines():
        autores = line.split(':::')[1]
        title = line.split(':::')[2]

        # por autor
        for autor in autores.split('::'):
            # frase
            for phrase in title.splitlines():
                # palavra
                for word in phrase.split():
                    if word not in sw:
                        yield autor + ' :: ' + word, 1

# REDUCE
# Name node especifica workers para executar a função REDUCE após MAP ser realizado
# Processar o valor das chaves contruída na função MAP
def reducefn(k, v):
    return sum(v)

s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

results = s.run_server(password="changeme")

w = csv.writer(open('result2.csv', 'w'))

L = list()
for k, v in results.items():

    a = k.split(' :: ')[0]
    print(a)

    if a not in L:
        w.writerow([k.split(' :: ')[0]])
        w.writerow([k.split(' :: ')[1], str(v)])
        L.append(a)
    else:
        w.writerow([k.split(' :: ')[1], str(v)])

