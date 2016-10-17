def produce_ecl(id):
    ecl_template = '''
IMPORT $;
IMPORT Std;

STRING15 input_firstname := 'MARY' : STORED('FirstName');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Sorted.FilePlus_FirstName_{},
            $.DeclareData.Person_Sorted.IDX_FirstName_{}(FirstName=input_firstname),
            RIGHT.RecPos);

//OUTPUT(CHOOSEN(F1,input_num,RANDOM()%256));
OUTPUT(CHOOSEN(F1,input_num));
'''

    output_path = "SequentialSearch_FirstName_{}.ecl".format(id)
    with open(output_path, 'w') as f:
        f.write(ecl_template.format(id, id))
        f.write("\n")


if __name__ == "__main__":
    for id in range(1, 65):
        produce_ecl(id)