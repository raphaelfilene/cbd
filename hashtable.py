from table import Table
from collections import defaultdict

class HashTable(Table):
    def __init__(self, datafile: str, filename: str = "HashTable.txt",
                 index: str = "SQ_CANDIDATO", hash_size: int = 1000):
        self.hash_size = hash_size
        super().__init__(datafile, filename)

    def write_to_file(self):
        to_write = [""] * self.hash_size
        for record in self.data.iterrows():
            hash_index = int(record[1][self.index]) % self.hash_size
            for key, value in self.columns.items():
                # write padded fields
                to_write[hash_index] += (("{:*<" + str(value) + "}").format(record[1][key]))
            to_write[hash_index] += ";" # separator, likely unneccessary
        to_write = [x + "\n" for x in to_write]

        # writing file
        with open(self.filename, 'w', encoding="latin1") as f:
            f.writelines(to_write)
            f.close()

    def hash_join(self, table: Table, index: str = ""):
        field = self.index if not index else index

        hash_joined = defaultdict(list)

        for record in self.data.iterrows():
            try:
                hvalue = int(record[1][field]) % self.hash_size
                same_hash = table.data[table.data[field] % self.hash_size == hvalue]
                for row in same_hash.iterrows():
                    if record[1][field] == row[1][field]:
                        hash_joined[hvalue].append(record)
            except:
                continue


        return hash_joined


t1 = HashTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_SP_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/hashsp.txt")

t2 = HashTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2019_RJ_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/hashrj.txt")

t1.hash_join(t2, "NR_PARTIDO")
