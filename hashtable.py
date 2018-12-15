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

    def hash_join(self, table: Table):
        hash_joined = defaultdict(list)
        for s in table1:
            h[s[index1]].append(s)
        return [(s, r) for r in table2 for s in h[r[index2]]]
        raise NotImplementedError
