from pyspark.sql import Column, Window, WindowSpec
from pyspark.sql import functions as f
from minsait.ttaa.datio.common.naming.Field import Field
from minsait.ttaa.datio.common.naming.PlayerInput import team_position, height_cm, short_name, age, potential, overall, \
    nationality
from minsait.ttaa.datio.common.Constants import PLAYER_CAT_A, PLAYER_CAT_B, PLAYER_CAT_C, PLAYER_CAT_D


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


class PlayerCat(Field):
    name = "player_cat"

    def build(self) -> Column:
        w: WindowSpec = Window \
            .partitionBy(team_position.column(), nationality.column()) \
            .orderBy(overall.column().desc())

        rank: Column = f.rank().over(w)

        built_column = f.when(rank < 3, PLAYER_CAT_A) \
            .when(rank < 5, PLAYER_CAT_B) \
            .when(rank < 10, PLAYER_CAT_C) \
            .otherwise(PLAYER_CAT_D) \
            .alias(self.name)

        return built_column


class PotentialVsOverall(Field):
    name = "potential_vs_overall"

    def build(self) -> Column:
        return division_rule(potential.column(), overall.column()).alias(self.name)


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


def division_rule(col1: Column, col2: Column) -> Column:
    return col1 / col2


cat_height_by_position = CatHeightByPosition()
potential_vs_overall = PotentialVsOverall()
player_cat = PlayerCat()
field_one = FieldOne()
field_two = FieldTwo()
