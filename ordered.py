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

    def nested_join(self, table: Table):
        raise NotImplementedError

    def merge_join(self, table: Table):
        raise NotImplementedError
