from table import Table


class OrderedTable(Table):
    def __init__(self, datafile: str, filename: str = "OrderedTable.txt",
                 index: str = "SQ_CANDIDATO"):
        super().__init__(datafile, filename)

    def write_to_file(self):
        with open(self.filename, 'w', encoding="latin1") as f:
            for record in self.data.iterrows():
                for key, value in self.columns.items():
                    # write padded fields
                    f.write(("{:*<" + str(value) + "}").format(record[1][key]))
                f.write("\n")
            f.close()

    def nested_join(self, table: Table, index: str = ""):
        raise NotImplementedError

    def merge_join(self, table: Table, index: str = ""):
        field = self.index if not index else index

        data = self.data.set_index(field).join(
                table.data.set_index(field), lsuffix='left', rsuffix='right',
                )
        merged = []


        with open("orderedjoin.txt", "w") as f:
            for record in data.iterrows():
                merge_value = ""

                # padding values
                for key, value in self.columns.items():
                    try:
                        merge_value += ("{:*<" + str(value) + "}").format(record[1][key])
                    except KeyError:
                        merge_value += ("{:*<" + str(value) + "}").format(record[0])

                merged.append(merge_value + "\n")
            f.writelines(merged)
            f.close()


t1 = OrderedTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2018_SP_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/orderedsp.txt")

t2 = OrderedTable("/home/ramon/Documents/PycharmProjects/cbd/consulta_cand_2018/consulta_cand_2019_RJ_modified.csv",
                "/home/ramon/Documents/PycharmProjects/cbd/orderedrj.txt")

# t1.nested_join(t2, "NR_PARTIDO")
t1.merge_join(t2, "NR_CANDIDATO")