IMPORT $;
IMPORT Std;

STRING10 input_state := 'NC' : STORED('State');
UNSIGNED4 input_num := 100 : STORED('Num');

F1 := FETCH($.DeclareData.Person_Unsorted.FilePlus_State,
	$.DeclareData.Person_Unsorted.IDX_State(State=input_state),
	RIGHT.RecPos);

OUTPUT(CHOOSEN(F1,input_num,RANDOM()%128));
