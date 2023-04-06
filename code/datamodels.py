"""
This script conatins data_models for the given datasets. 
Made using pydantic 
"""

import pydantic
from pathlib import Path
from datetime import datetime
# pydantic data model for illumina data

class IlluminaDataModel(pydantic.BaseModel):
    sample_id: str
    sample_id_suffix: str
    seq: str
    sample_len: str
    read_type: str
    filename: str

    @pydantic.validator('sample_id')
    def clean_sample_id(cls,v):
        """ cleans the sample id removing non standard characters"""
        clean_id = v.split(" ")[-1]
        if not all([a.isnumeric() for a in clean_id.split("-")]):
            raise ValueError(f"sample id is non standard: {clean_id}")
        else:
            return clean_id.split("-")[0]
        
    @pydantic.validator('sample_id_suffix')
    def clean_sample_id_suffix(cls,v):
        """ cleans the sample id removing non standard characters"""
        clean_id = v.split(" ")[-1]
        if not all([a.isnumeric() for a in clean_id.split("-")]):
            raise ValueError(f"sample id is non standard: {clean_id}")
        else:
            return "-".join(clean_id.split("-")[1:])
        

    @pydantic.validator('seq')
    def check_sequence(cls,v):
        """ checks that the sequence is in format letter:digit(s)"""
        if (v[0].isnumeric()) or (not v[1:].isnumeric()):
            raise ValueError( f"sequence is non standard: {v}")
        else:
            return v
        
    @pydantic.validator('sample_len')
    def check_length(cls,v):
        """ checks that the sequence is in format letter:digit(s)"""
        if (v[0].isnumeric()) or (not v[1:-1].isnumeric()):
            raise ValueError( f"sequence is non standard: {v}")
        else:
            return v
        
    @pydantic.validator('read_type')
    def clean_read_type(cls,v):
        """ checks that the sequence is in format letter:digit(s)"""
        if v[0].isnumeric() or (not v[1:].isnumeric()):
            raise ValueError( f"sequence is non standard: {v}")
        else:
            return int(v[1:])

    
    @pydantic.validator('filename')
    def check_filename(cls, v):
        """check filename for correct extension ans beginning"""
        clean_fname= v.split(" ")[-1]
        if (not clean_fname.endswith(".gz")) or (not clean_fname[0].isnumeric()):
            raise ValueError(f"filename is non standard: {clean_fname}")
        else:
            return clean_fname
        
# nanopore datamodel
class NanoporeDataModel(pydantic.BaseModel):
    seq_date: str
    barcode_number:str
    path_to_seq_file: str


    @pydantic.validator('seq_date')
    def transform_date(cls, v):
        """transforms str to date"""
        clean_date = datetime.strptime(v, "%Y%m%d")
        return clean_date.strftime("%Y%m%d")


    @pydantic.validator('barcode_number')
    def transform_barcode(cls, v):
        if not v.isnumeric():
            raise ValueError(f"barcode is not numeric: {v}")
        return int(v)

    @pydantic.validator('path_to_seq_file')
    def check_path(cls, v):
        """checks that path is actually a consistent path using pathlib"""
        my_path=Path(v)
        return v

