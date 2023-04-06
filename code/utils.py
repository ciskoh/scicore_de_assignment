"""Utility / lower functions to:
- extract files from source
- transform data
- connect to  database"""


import pydantic
from typing import List, Tuple
import pandas as pd
from io import StringIO
from Bio import SeqIO
import sqlite3


#------------  EXTRACT -------------------

def extract_ill_data(
        fp: str, 
        dataModel: pydantic.BaseModel
        )->Tuple[ List[pydantic.BaseModel], List[dict] ]:
    """opens a tree file and imports the data following given 
    pydantic data model
    
    Returns:
    list of objects as per data model, list of errors

    """
    data = []
    errors=[]
    # Open the .tree file and read its contents
    with open(fp, 'r') as f:
        for line in f:
            line=line.rstrip('\n')
            
            if line.endswith("gz"):
                components = line.strip().split('_')
                try:
                    obj =dataModel(
                        sample_id=components[0], 
                        sample_id_suffix=components[0],
                        seq=components[1], 
                        sample_len=components[2], 
                        read_type=components[3], 
                        filename=line,
                    )
                    data.append(obj)
                except pydantic.ValidationError as e:
                    errors.append({"line": line, "error":str(e)})

    return data, errors



def extract_nanopore_data(
    filepath: str, dataModel: pydantic.BaseModel
) -> Tuple[List[pydantic.BaseModel], List[dict]]:
    """
    loads nanopore data from source file checking consistency 
    using the given pydantic data model

    Returns list of objects, list of errors

    """

    data = []
    errors = []
    # Open the .tree file and read its contents
    with open(filepath, "r") as f:
        for line in f:
            line = line.rstrip("\n")
            if line.startswith("nanopore"):
                parent = line
                seq_date = parent.split("/")[1].split("_")[0]
            else:
                components = line.split(" ")[-1].split(".")
                try:
                    obj = dataModel(
                        seq_date=seq_date,
                        barcode_number=components[0][-2:],
                        path_to_seq_file=f"{parent}/{line.split(' ')[-1]}",
                    )
                    data.append(obj)
                except pydantic.ValidationError as e:
                    errors.append({"line": line, "date": seq_date, "error": str(e)})
    return data, errors

#------------  TRANSFORM -------------------

def fasta_to_df(fasta_str:str)->pd.DataFrame:
    """reads a fasta string to a dataframe extracting selected properties"""
    
    fasta_io = StringIO(fasta_str) 
    records = SeqIO.parse(fasta_io, "fasta")

    output_list= []
    for r in records:
        fasta_dict ={
        'id': r.id,
        'description': r.description,
        'name': r.name,
        }
        output_list.append(fasta_dict)

    return pd.DataFrame(output_list)


#------------  CONNECT -------------------


def create_connection(db_file):
    """ create a database connection to a SQLite database """
    conn = None
    try:
        conn = sqlite3.connect(db_file)
        print(sqlite3.version)
        return conn
    except sqlite3.Error as e:
        print(e)
    
if __name__ == "__main__":
    pass
