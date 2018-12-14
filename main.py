#%%
import pandas as pd
from collections import OrderedDict
from sys import argv
from bplustree import BPlusTree

#%%
# arquivo = "/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_BR.csv" if len(argv) < 2 else argv[1]
arquivo = "/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_SP_modified.csv"
colunas= ['PERIODO','UF','MUNICIPIO','COD_MUNICIPIO_TSE','NR_ZONA','SEXO','FAIXA_ETARIA','GRAU_ESCOLAR','TOTAL']
# data = pd.read_csv(arquivo, header= None, names=colunas)
data = pd.read_csv(arquivo, sep=",", header=0, encoding="latin1")
df = pd.DataFrame(data)

#%%
# for column in df:
#     print(df[df[column]==df[column].max()])

#%%
# for i in df:
#     print(hash(i))

#%%
columns = OrderedDict()
columns["SQ_CANDIDATO"] = 12  # index
columns["DT_GERACAO"] = 10
columns["SG_UF"] = 2
columns["DS_CARGO"] = 18
columns["NR_CANDIDATO"] = 5
columns["NM_CANDIDATO"] = 50
columns["NR_CPF_CANDIDATO"] = 11
columns["DS_SITUACAO_CANDIDATURA"] = 6
columns["DS_DETALHE_SITUACAO_CAND"] = 22
columns["TP_AGREMIACAO"] = 15
columns["NR_PARTIDO"] = 2
columns["SG_PARTIDO"] = 13
columns["SG_UF_NASCIMENTO"] = 2
columns["DT_NASCIMENTO"] = 10
columns["NR_IDADE_DATA_POSSE"] = 2
columns["NR_TITULO_ELEITORAL_CANDIDATO"] = 12
columns["DS_GENERO"] = 9
columns["DS_GRAU_INSTRUCAO"] = 29
columns["DS_ESTADO_CIVIL"] = 25
columns["DS_COR_RACA"] = 8
columns["DS_SIT_TOT_TURNO"] = 13
columns["ST_REELEICAO"] = 1

def get_register_size(columns):
    return sum(columns.values()) + 1 # \n character

class OrderedTable(object):
    def __init__(self, datafile: str, filename: str = "OrderedTable.txt",
                 index: str = "SQ_CANDIDATO"):
        self.datafile = datafile
        self.filename = filename
        self.index = index

        self.data = pd.read_csv(datafile, sep=",", header=0,
                                encoding="latin1").sort_values(index)

        # writing file
        with open(self.filename, 'w', encoding="latin1") as f:
            for record in self.data.iterrows():
                for key, value in columns.items():
                    # write padded fields
                    f.write(("{:*<" + str(value) + "}").format(record[1][key]))
                f.write("\n")
            f.close()

new_table = OrderedTable(arquivo)


#%%
class HashTable(object):
    def __init__(self, datafile: str, filename: str = "HashTable.txt",
                 index: str = "SQ_CANDIDATO", hash_size: int = 1000):
        self.datafile = datafile
        self.filename = filename
        self.index = index

        self.data = pd.read_csv(datafile, sep=",", header=0,
                                encoding="latin1").sort_values(index)

        to_write = [""] * hash_size
        for record in self.data.iterrows():
            hash_index = int(record[1][index]) % hash_size
            for key, value in columns.items():
                # write padded fields
                to_write[hash_index] += (("{:*<" + str(value) + "}").format(record[1][key]))
            to_write[hash_index] += ";" # separator, likely unneccessary
        to_write = [x + "\n" for x in to_write]


        # writing file
        with open(self.filename, 'w', encoding="latin1") as f:
            f.writelines(to_write)
            f.close()


new_table = HashTable(arquivo)

#%%
class BtreeTable(object):
    def __init__(self, datafile: str,
                 filename: str = "/home/ramon/Documents/PycharmProjects/cbd/BtreeTable.db",
                 index: str = "SQ_CANDIDATO"):
        self.datafile = datafile
        self.filename = filename
        self.index = index

        self.data = pd.read_csv(datafile, sep=",", header=0,
                                encoding="latin1").sort_values(index)
        
        # this file is a bit different from the others
        # it can be written ahead without being closed
        # which is why it will become a persistent attribute
        self.tree = BPlusTree(self.filename, order=50)
        
        # dataframe still needs to be parsed line by line
        for record in self.data.iterrows():
            btree_value = ""
            
            # padding values
            for key, value in columns.items():
                btree_value += ("{:*<" + str(value) + "}").format(record[1][key])

            # converting to binary
            btree_value = btree_value.encode()
            
            # B-tree indices must be integers
            tree_index = int(record[1][index])
            self.tree[tree_index] = btree_value
                
        # commits changes without closing file
        self.tree.get(len(self.tree))
        self.tree.checkpoint()
    
    # closes file and erases write-ahead log
    def close(self):
        self.tree.close()


new_table = BtreeTable(arquivo)

# load a single record to memory (parameter is index value)
new_table.tree.get(250000600364)

# load a range of values to memory
new_table.tree[250000600365:250000600369]

# loop through records without loading all of them to memory
for key, value in new_table.tree.items():
    pass

# load entire db to memory (not recommended)
# list(new_table.tree.items())

# commit changes, close db and erases log
new_table.close()


#%%
num_records_per_block = 5

def bloco(n=1):
    global num_records_per_block
    global arquivo
    global colunas
    skip = (int(n)-1)*num_records_per_block
    bloco = pd.read_csv(arquivo,skiprows = skip,header = None,  nrows= num_records_per_block,names = colunas)
    return bloco

bloco(1)

#%%
num_records = df.shape[0]
num_blocks  = (num_records//num_records_per_block)+1
campo_chave = 'PERIODO'   #não é chave, só teste
campo_ord = 'PERIODO'     #só teste

#%%
