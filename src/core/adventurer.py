import random

class Adventurer:
    def __init__(self, name, guild, level):
        self.name = name
        self.guild = guild
        self.level = level
        self.attack = random.randint(5, 15) + level

    def attack_roll(self):
        return self.attack + random.randint(-3, 3)