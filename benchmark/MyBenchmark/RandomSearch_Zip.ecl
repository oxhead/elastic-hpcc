IMPORT $;
IMPORT Std;

STRING10 input_zip := '00603' : STORED('Zip');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Unsorted.FilePlus_Zip,
	$.DeclareData.Person_Unsorted.IDX_Zip(Zip=input_zip),
	RIGHT.RecPos);

OUTPUT(CHOOSEN(F1,input_num,RANDOM()%64));
