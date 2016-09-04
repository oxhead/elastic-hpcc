IMPORT $;
IMPORT Std;

STRING20 input_city := 'ABBEVILLE' : STORED('City');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Unsorted.FilePlus_City,
	$.DeclareData.Person_Unsorted.IDX_City(City=input_city),
	RIGHT.RecPos);

//OUTPUT(CHOOSEN(F1,input_num,RANDOM()%64));
OUTPUT(CHOOSEN(F1,input_num));
