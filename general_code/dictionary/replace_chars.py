char = "Mc Donald's Daily/Daily by Branch (with date filter)"
char_2 = char.replace("/", "_and_").replace("'",
                                            "").replace("(", "").replace(")", "").replace(" ", "_")

print(char)
print(char_2)
