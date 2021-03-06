import sys

"""
usage of the script:
python3 generate_membership_file.py number_of_processes number_of_messages
any other usage is going to result in an undefined behaviour.
"""

def pad_with_zero(i):
    if i<10:
        return "00" + str(i)
    if i < 100:
        return "0" + str(i)
    return str(i)

if __name__=='__main__':
    number_of_processes = int(sys.argv[1])
    number_of_dependencies = int(sys.argv[2])

    f = open('membership', 'w')
    f.write(str(number_of_processes) + "\n")
    # for i in range(1, number_of_processes+1):
    #     f.write(str(i) + " ")
    #     f.write('127.0.0.1')
    #     f.write(" ")
    #     f.write("12" + pad_with_zero(i) + "\n")

    for i in range(1, number_of_processes+1):
        dependencies = [ str((i + x)%number_of_processes + 1) for x in range(number_of_dependencies)]
        dep_str = ""
        for j, el in enumerate(dependencies):
            dep_str += str(el) + (" " if j != len(dependencies) - 1 else "")
        print(i, dep_str)
        f.write(str(i) + " " + dep_str )
        f.write("\n")

def generate_memb_ste(number_of_processes, number_of_dependencies):
    f = open('membership', 'w')
    f.write(str(number_of_processes) + "\n")
    # for i in range(1, number_of_processes+1):
    #     f.write(str(i) + " ")
    #     f.write('127.0.0.1')
    #     f.write(" ")
    #     f.write("12" + pad_with_zero(i) + "\n")

    dep = {}
    for i in range(1, number_of_processes+1):
        dependencies = [ str((i + x)%number_of_processes + 1) for x in range(number_of_dependencies)]
        dep[i] = [int(d) for d in dependencies]
        dep_str = ""
        for j, el in enumerate(dependencies):
            dep_str += str(el) + (" " if j != len(dependencies) - 1 else "")
        f.write(str(i) + " " + dep_str )
        f.write("\n")
    return dep

def create_memb_from_dict(causalDep, number_of_processes):
    f = open('membership', 'w')
    f.write(str(number_of_processes) + "\n")
    for i in range(1, number_of_processes+1):
        dependencies = [ str(x) for x in causalDep[i]]
        dep_str = ""
        for j, el in enumerate(dependencies):
            dep_str += str(el) + (" " if j != len(dependencies) - 1 else "")
        f.write(str(i) + " " + dep_str )
        f.write("\n")
    return