import random


class Monster:
    def __init__(self, species, danger_level):
        self.species = species
        self.danger_level = danger_level

    def defense_roll(self):
        return self.danger_level * 2 + random.randint(-2, 2)
