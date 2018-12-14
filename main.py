#%%
from sys import argv
import pandas as pd
from ordered import OrderedTable
from hashtable import HashTable
from btree import BtreeTable


#%%
# arquivo = "/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_BR.csv" if len(argv) < 2 else argv[1]
arquivo = "/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_SP_modified.csv"
data = pd.read_csv(arquivo, sep=",", header=0, encoding="latin1")
df = pd.DataFrame(data)


#%%
new_table = OrderedTable(arquivo)


#%%
new_table = HashTable(arquivo)


#%%
new_table = BtreeTable(arquivo)

# load a single record to memory (parameter is index value)
new_table.tree.get(250000600364)

# load a range of values to memory
new_table.tree[250000600365:250000600369]

# loop through records without loading all of them to memory
for key, value in new_table.tree.items():
    pass

# load entire db to memory (not recommended)
list(new_table.tree.items())

# commit changes, close db and erases log
new_table.close()
