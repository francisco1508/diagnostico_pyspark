from pyspark.sql import Column
from pyspark.sql.functions import *


class Field:
    def __init__(self, *args):
        if len(args) > 0:
            self.name: str = args[0]
        else:
            self.name: str

    def column(self) -> Column:
        return col(self.name)
