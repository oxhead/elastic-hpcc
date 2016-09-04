IMPORT $;
IMPORT Std;

STRING25 input_lastname := 'ABBOTT' : STORED('LastName');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Sorted.FilePlus_LastName,
	$.DeclareData.Person_Sorted.IDX_LastName(LastName=input_lastname),
	RIGHT.RecPos);

//OUTPUT(CHOOSEN(F1,input_num,RANDOM()%256));
OUTPUT(CHOOSEN(F1,input_num));
