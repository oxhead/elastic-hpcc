IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
UNSIGNED4 input_num := 100 : STORED('Num');

idx1 := $.DeclareData.IDX__Person_LastName;
idx1_filtered  := idx1(
		KEYED(input_last_name='' OR LastName=input_last_name)
);

f1 := FETCH(
    $.DeclareData.Person.FilePlus,
    TOPN(idx1_filtered, input_num, RecPos),
    RIGHT.RecPos
);

//OUTPUT(COUNT(f1));
OUTPUT(f1);