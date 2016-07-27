IMPORT $;
IMPORT Std;

STRING10 input_first_name := 'MARY' : STORED('FirstName');
UNSIGNED4 input_num := 100 : STORED('Num');

//idx := $.DeclareData.IDX_Person_FirstName;
//idx_filtered := idx(Std.Str.Contains(FirstName, input_first_name, TRUE));

F1 := FETCH($.DeclareData.Person_Unsorted.FilePlus_FirstName,
	//idx_filtered,
	$.DeclareData.Person_Unsorted.IDX_FirstName(FirstName=input_first_name),
	RIGHT.RecPos);

OUTPUT(CHOOSEN(F1,input_num,RANDOM()%256));
