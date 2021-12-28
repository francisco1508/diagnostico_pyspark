from pyspark.sql import Column, Window, WindowSpec
from pyspark.sql import functions as f
from minsait.ttaa.datio.common.naming.Field import Field
from minsait.ttaa.datio.common.naming.PlayerInput import team_position, height_cm, short_name, age


class CatHeightByPosition(Field):
    name = "cat_height_by_position"

    def build(self) -> Column:
        w: WindowSpec = Window \
            .partitionBy(team_position.column()) \
            .orderBy(height_cm.column().desc())
        rank: Column = f.rank().over(w)
        built_column = f.when(rank < 10, "A") \
            .when(rank < 50, "B") \
            .otherwise("C") \
            .alias(self.name)

        return built_column


class FieldOne(Field):
    name = "field_one"

    def build(self) -> Column:
        return magic_rule(short_name.column(), age.column()).alias(self.name)


class FieldTwo(Field):
    name = "field_two"

    def build(self) -> Column:
        return magic_rule(short_name.column(), team_position.column()).alias(self.name)


def magic_rule(col1: Column, col2: Column) -> Column:
    return f.upper(f.concat(col1, col2))


cat_height_by_position = CatHeightByPosition()
field_one = FieldOne()
field_two = FieldTwo()
