#coding: latin-1
from six.moves import cPickle
import pandas as pd
import csv
import shutil
import json
import os

file_candidato_csv = "/home/me/Documents/Cons_BD/candidatos/consulta_cand_2018_BRASIL.csv"
file_candidato_txt = "/home/me/Documents/Cons_BD/candidatos/consulta_cand_2018_BRASIL.txt"
file_eleitorado = "/home/me/Documents/Cons_BD/eleitorado/perfil_eleitorado_ATUAL.txt"

'''

class BD_heap ():
    arquivo = None
    df = None
    num_records_per_block = 80
    num_records = None
    num_blocks = None


    def __init__(self, file, s=1):
        self.arquivo = file
        if s==1:
            #arquivo = file_candidato_csv
            #colunas = ['PERIODO', 'UF', 'MUNICIPIO', 'COD_MUNICIPIO_TSE', 'NR_ZONA', 'SEXO', 'FAIXA_ETARIA', 'GRAU_ESCOLAR','TOTAL']
            data = pd.read_csv(self.arquivo, header=0, sep=';')
            self.df = pd.DataFrame(data)
            self.num_records = self.df.shape[0]
            self.num_blocks = (self.num_records // self.num_records_per_block) + 1
            campo_chave = 'key'
            campo_ord = 'PERIODO'
            print ("Banco - Candidato\n")
        elif s==2:
            #Eleitorada
            print ("Banco - Eleitorado\n")
        else:
            return 0

    def bloco(self,n=1):
        skip = (int(n) - 1) * self.num_records_per_block
        bloco = pd.read_csv(self.arquivo,skiprows = skip,header = 0, sep=';', nrows= self.num_records_per_block)
        return bloco

    def search_on_line_block(self, line, bloco_n, campo):
        return self.bloco(bloco_n).loc[line, campo]

    def select_key_ord(campo, value):
        global campo_chave
        global campo_ord
        campo = campo
        value = value
        if campo == campo_ord:
            return search_on_block(num_blocks // 2, campo, value)
'''

#bd_heap = BD_heap(file_candidato, 1)
#print (bd_heap.bloco())

#blocos = [0]
#num_records = bd_heap.df.shape[0]
#num_blocks = (num_records//bd_heap.num_records_per_block)+1
#campo_chave = 'key'
#campo_ord = 'PERIODO'
#print("Nº de Registros: " + str(num_records) + ", Nº de Blocos: " + str(num_blocks))
#print bd_heap.df

#TRANSFORMANDO CSV EM TXT
with open(file_candidato_txt, "w") as my_output_file:
    with open(file_candidato_csv, "r") as my_input_file:
        [my_output_file.write(" ".join(row) + '\n') for row in csv.reader(my_input_file)]
        my_output_file.close()

#SEPARANDO EM ARQUIVOS TXT
bloco = 1
num_records = 0
num_records_secundario = 0
header = None
try:
    shutil.rmtree("bd_heap")
except Exception:
    pass
os.mkdir("bd_heap")

with open(file_candidato_txt) as fp:
     line = fp.readline()
     e_header = 0
     data = {}
     while line:
         list_line = line.split(';')
         line = fp.readline()
         if e_header==1:
             num_records+=1
             num_records_secundario+=1
             data[num_records] = []
             for x,y in enumerate(header):
                 data[num_records].append({
                     y.decode('latin-1'):list_line[x].decode('latin-1')
                 })
             if num_records_secundario==80: #Tamanho bloco

                 with open('bd_heap/data' + str(bloco) + '.txt', 'w') as outfile:
                     json.dump(data, outfile)
                 data = {}
                 num_records_secundario = 0
                 bloco += 1


         else:
             header = list_line
             e_header = 1
     with open('bd_heap/data' + str(bloco) + '.txt', 'w') as outfile:
         json.dump(data, outfile)
     data = {}

print ("N Records: " + str(num_records))
print ("Blocos: " + str(bloco))
print ("Ultimo Bloco: " + str(num_records_secundario))



