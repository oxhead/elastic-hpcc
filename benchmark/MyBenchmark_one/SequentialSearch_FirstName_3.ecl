
IMPORT $;
IMPORT Std;

STRING15 input_firstname := 'MARY' : STORED('FirstName');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Sorted.FilePlus_FirstName_3,
            $.DeclareData.Person_Sorted.IDX_FirstName_3(FirstName=input_firstname),
            RIGHT.RecPos);

//OUTPUT(CHOOSEN(F1,input_num,RANDOM()%256));
OUTPUT(CHOOSEN(F1,input_num));

