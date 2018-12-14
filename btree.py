from bplustree import BPlusTree
from table import Table


class BtreeTable(Table):
    def __init__(self, datafile: str,
                 filename: str = "/home/ramon/Documents/PycharmProjects/cbd/BtreeTable.db",
                 index: str = "SQ_CANDIDATO"):
        super().__init__(datafile, filename)

    def write_to_file(self):
        # this file is different: it can be written ahead without being closed
        # which is why it will become a persistent attribute
        self.tree = BPlusTree(self.filename, order=50)

        # dataframe still needs to be parsed line by line
        for record in self.data.iterrows():
            btree_value = ""

            # padding values
            for key, value in self.columns.items():
                btree_value += ("{:*<" + str(value) + "}").format(record[1][key])

            # converting to binary
            btree_value = btree_value.encode()

            # B-tree indices must be integers
            tree_index = int(record[1][self.index])
            self.tree[tree_index] = btree_value

        # commits changes without closing file
        self.tree.get(len(self.tree))
        self.commit()

    def commit(self):
        return self.tree.checkpoint()

    # closes file and erases write-ahead log
    def close(self):
        self.tree.close()

    def btree_join(self, table: Table):
        raise NotImplementedError
