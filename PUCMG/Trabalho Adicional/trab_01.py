import glob
import mincemeat
import csv

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

    from nltk.corpus import stopwords
    sw = stopwords.words("english")

    for line in v.splitlines():
        autores = line.split(':::')[1]
        title = line.split(':::')[2]

        # por autor
        for autor in autores.split('::'):

            if (autor == 'Grzegorz Rozenberg') or (autor == 'Philip S. Yu'):
                # frase
                for phrase in title.splitlines():
                    # palavra
                    for word in phrase.split():
                        word = str(word).replace('.', '').replace(',', '').replace("'", '').replace(':', '').replace('?', '').lower()
                        if word not in sw:
                            yield autor + ' :: ' + word, 1

# REDUCE
# Name node especifica workers para executar a função REDUCE após MAP ser realizado
# Processar o valor das chaves contruída na função MAP
def reducefn(k, v):
    return sum(v)

# Server
s = mincemeat.Server()
s.datasource = source
s.mapfn = mapfn
s.reducefn = reducefn

# Resultado Reduce
results = s.run_server(password="changeme")

w = csv.writer(open('result2.csv', 'w'))

L = list()
P = {}
P2 = {}
G = {}
F = True

# 2 autores e seus termos
for k, v in results.items():

    a = k.split(' :: ')[0]

    if a == 'Grzegorz Rozenberg':
        P[k.split(' :: ')[1]] = v

    if a == 'Philip S. Yu':
        P2[k.split(' :: ')[1]] = v

    G['Grzegorz Rozenberg'] = P
    G['Philip S. Yu'] = P2

# 2 maiores pontuadores de cada
M = list()
for item in G.values():
    for dict_item in sorted(item, key=item.get, reverse=True):
        M.append(item[dict_item])

    maximo = max(M)

    for dict_item in sorted(item, key=item.get, reverse=True):
        if item[dict_item] == maximo:
            print(dict_item)

    M = list()
