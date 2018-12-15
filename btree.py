from itertools import zip_longest, tee
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

    def btree_join(self, table: Table, index: str = ""):
        '''
            Join BtreeTables without loading both of them to memory.
        '''
        field = self.index if not index else index

        joined_tree = BPlusTree("/home/ramon/Documents/PycharmProjects/cbd/joined_result.db", order=50)

        if field == self.index: # index-based join
            joined_tree.batch_insert((
                (i, self.tree.get(i))
                for i in filter(lambda x: x in self.tree.keys(),
                                               table.tree.keys())
            ))
        else:
            data = self.data.set_index(field).join(
                table.data.set_index(field), lsuffix='left', rsuffix='right',
                how='inner')
            for record in data.iterrows():
                btree_value = ""

                # padding values
                for key, value in self.columns.items():
                    try:
                        btree_value += ("{:*<" + str(value) + "}").format(record[1][key])
                    except KeyError: # None value
                        continue

                # converting to binary
                btree_value = btree_value.encode()

                # B-tree indices must be integers
                try:
                    tree_index = int(record[1][index])
                except KeyError: # None value
                    continue
                joined_tree.tree[tree_index] = btree_value

            # commits changes without closing file

        joined_tree.close()

        return True



t1 = BtreeTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_SP_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/btreesp.db")

t2 = BtreeTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2019_RJ_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/btreerj.db")

t1.btree_join(t2, "NR_PARTIDO")
