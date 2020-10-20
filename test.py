with open("output_validate/proc01.output", "r") as f1:
    s1 = set(f1.readlines())
with open("output_validate/proc02.output", "r") as f2:
    s2 = set(f2.readlines())
print(s1.difference(s2))