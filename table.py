from collections import OrderedDict
import pandas as pd


class Table():
    # initializing columns
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

    def get_register_size(self):
        return sum(self.columns.values()) + 1 # \n character

    def __init__(self, datafile: str, filename: str,
                 index: str = "SQ_CANDIDATO"):
        self.datafile = datafile
        self.filename = filename
        self.index = index

        self.data = pd.read_csv(datafile, sep=",", header=0,
                                encoding="latin1").sort_values(index)

        self.write_to_file()

    def write_to_file(self):
        raise NotImplementedError
