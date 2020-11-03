with open("output_validate/proc05.output", "r") as f1:
    s1 = set(f1.readlines())
with open("output_validate/proc07.output", "r") as f2:
    s2 = set(f2.readlines())
print(sorted(list(s1.difference(s2))))