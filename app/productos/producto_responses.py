from pydantic import BaseModel

class ProductoControls(BaseModel):
    control_name:str
    value:str