""" 
    high level functions to complete the exercises:

    ## Exercise 1
    write a script (e.g. in Python) to parse the four provided files and 
    create an SQL database (e.g. local SQLite DB) to store the key information 
    from the dataset.

    ## Exercise 2
    write a script which joins the tables to list all files (file paths) 
    belonging to a species.

    ## Exercise 3
    as a way to enrich the data in this dataset, find all proteins in 
    UniProtKB which are annotated with the antibiotic Imipenem for the 
    organism Klebsellia pneumoniae (TaxID 573, annotated as "Klebs" in 
    the previous tables). To accomplish this, please write a script using 
    the UniProtKB API toget this information and insert it as a table in the 
    same SQL database.

"""


##  ---  EXERCISE 1 Load tables to DB  ---
from pathlib import Path
from code import datamodels as dm
from code import utils 
import pandas as pd
import requests
import argparse

def execute_exercise_1(src_folder, db_path):
    """
        write a script (e.g. in Python) to parse the four provided files and 
        create an SQL database (e.g. local SQLite DB) to store the key information 
        from the dataset.
    """

    # create db
    conn = utils.create_connection(db_path)

    #load and ingest table1
    t1_src= Path(src_folder) / "illumina_data.tree"
    t1_list, errs = utils.extract_ill_data(t1_src, dm.IlluminaDataModel)
    if len(errs) >0:
        for er in errs:
            print(er["error"])

    t1_df = pd.DataFrame([a.dict() for a in t1_list])
    t1_df.columns= [c.lower() for c in t1_df.columns]

    t1_df.to_sql("illumina_data", conn, if_exists="replace" )

    #load and ingest table2
    t2_src= Path(src_folder) / "nanopore_data.tree"
    t2_list, errs = utils.extract_nanopore_data(t2_src, dm.NanoporeDataModel)
    if len(errs) >0:
        for er in errs:
            print(er["error"])

    t2_df = pd.DataFrame([a.dict() for a in t2_list])
    t2_df.columns= [c.lower() for c in t2_df.columns]
    t2_df.to_sql("nanopore_data", conn, if_exists="replace" )

    #load and ingest table3
    t3_src = Path(src_folder) / "MIC_data.csv"
    t3_df = pd.read_csv(t3_src)
    t3_df.columns= [c.lower().replace(" ","_").replace("#", "n") for c in t3_df.columns]

    t3_df.to_sql("mic_data", conn, if_exists="replace" )

    #load and ingest table4
    t4_src = Path(src_folder) / "sample_table.csv"
    t4_df = pd.read_csv(t4_src)
    t4_df.columns= [c.lower().replace(" ","_").replace("#", "n") for c in t4_df.columns]
    t4_df.to_sql("sample_table", conn, if_exists="replace" )

    print( "\n  -- EXERCISE 1 finished  --")
    cursor = conn.cursor()
    cursor.execute('SELECT name FROM sqlite_master WHERE type="table"')
    table_list = [a[0] for a in cursor.fetchall()]

    print(f"the following tables are available at {db_path}: {' - '.join(table_list)}" )


def execute_exercise_2(db_path: str, species_name: str, dst_path:str ="."):
    """
    write a script which joins the tables to list all files (file paths) 
    belonging to a species.
    Saves the output as csv table in dst_path
    """
    # create db conn
    conn = utils.create_connection(db_path)

    sql = """
    SELECT internal_n, substr(labor_number, 0, 7) as sample, species, sequenced, CAST(barcode as integer) as barcode, substr(CAST(resequenced as TEXT), 0 ,9) as resequenced, CAST(rebarcode AS INTEGER) as rebarcode, illumina_data.filename as illumina_file, nanopore_data.path_to_seq_file as nanopore_file, nano_reseq.path_to_seq_file as nanopore_file_resequenced
    FROM sample_table
    LEFT JOIN illumina_data ON sample = illumina_data.sample_id
    LEFT JOIN nanopore_data ON (sequenced = nanopore_data.seq_date AND barcode=nanopore_data.barcode_number)
    LEFT JOIN nanopore_data as nano_reseq ON (resequenced = nano_reseq.seq_date AND rebarcode=nano_reseq.barcode_number)
    WHERE species= ?"""

    params=(species_name,)
    results = pd.read_sql(sql, conn, params=params)
    output_path = Path(dst_path) / "exercise_2_output.csv"
    results.to_csv(output_path)

    print(" \n\n  --  EXERCISE 2 finished -- ")
    print("table header: ", results.head())
    print("complete output saved at: ", output_path  )


def execute_exercise_3(db_path: str, taxonomy:str="573", annotation:str="Imipenem"):
    """
        find all proteins in UniProtKB which are annotated with the antibiotic 
        Imipenem for the 
        organism Klebsellia pneumoniae (TaxID 573, annotated as "Klebs" in 
        the previous tables). To accomplish this, please write a script using 
        the UniProtKB API toget this information and insert it as a table in the 
        same SQL database.
    """
    _conn=utils.create_connection(db_path)

    # extract data
    url = f"https://rest.uniprot.org/uniprotkb/stream?compressed=false&format=fasta&query=%28%28taxonomy_id%3A{taxonomy}%29%20AND%20{annotation}%29"
    all_fastas = requests.get(url).text
    # transform
    fastas_df = utils.fasta_to_df(all_fastas)
    # laod to db
    fastas_df.to_sql("uni_prot", _conn, if_exists="replace" )

    print(" \n\n  --  EXERCISE 3 finished -- ")
    my_table= pd.read_sql_query("SELECT * from uni_prot", con=_conn)
    print("ingested table uni_prot", f"records_n: {len(my_table)}", f" attributes: {my_table.columns}" )

    _conn.close()

def main_wrapper():
     
    """
        Allows to launch exercises with CLI.

    """
    DEFAULT_SOURCE= str(Path("data") / "inputs")
    DEFAULT_DB_PATH = str(Path("data") / "outputs" / "scicore.db")

    args = argparse.ArgumentParser()

    # Add arguments
    args.add_argument(
        "-e", "--exercise", type=int, required=True, help="exercise number to execute, Int from 1 to 3"
    )
    args.add_argument(
        "-s", "--source", type=str, required=False, default=DEFAULT_SOURCE, help="source folder of raw data for exercise 1"
    )
    args.add_argument(
        "-d", "--pathtodb", type=str, required=False, default=DEFAULT_DB_PATH, help="path to database location"
    )
    args.add_argument(
        "-p", "--species", type=str, required=False, default='Kleb', help="Species to query for exercise 2'"
    )
    args.add_argument(
        "-t", "--taxonomyid", type=str, required=False, default='573', help="taxonomy code for exercise 3"
    )
    args.add_argument(
        "-a", "--annotation", type=str, required=False, default='Imipenem', help="annotation keyword to query for exercise 4"
    )

    args = args.parse_args()

    if args.exercise == 1:

        # run ex 1
        src_folder=args.source
        db_path=args.pathtodb

        print("  --  RUNNING EXERCISE 1 ---")
        print("parameters:", f"source folder: {src_folder}", f"db_path: {db_path}")
        execute_exercise_1(src_folder, db_path )

    elif args.exercise == 2:

        # run ex 2
        db_path=args.pathtodb
        species=args.species

        print("  --  RUNNING EXERCISE 2 ---")
        print("parameters:", f"species name: {species}", f"db_path: {db_path}")
        execute_exercise_2(db_path, species)

    elif args.exercise == 3:

        #run ex 3
        db_path=args.pathtodb
        tax = args.taxonomyid
        annotation = args.annotation
        print("  --  RUNNING EXERCISE 3 ---")
        print("parameters:", f"taxonomy: {tax}",f"annotation: {annotation}", f"db_path: {db_path}")
        execute_exercise_3(db_path=db_path, taxonomy=tax, annotation=annotation)

    else:
        raise ValueError("choose an exercise between 1 and 3. You chose : {args.exercise}")



if __name__ == "__main__":
    
  main_wrapper()





