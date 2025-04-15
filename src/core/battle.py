class Battle:
    def __init__(self, adventurer, monster):
        self.adventurer = adventurer
        self.monster = monster

    def simulate(self):
        attack = self.adventurer.attack_roll()
        defense = self.monster.defense_roll()
        result = "win" if attack > defense else "lose"

        return {
            "adventurer": self.adventurer.name,
            "guild": self.adventurer.guild,
            "level": self.adventurer.level,
            "attack_score": attack,
            "monster": self.monster.species,
            "danger_level": self.monster.danger_level,
            "defense_score": defense,
            "result": result
        }