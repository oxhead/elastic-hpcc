IMPORT $;
IMPORT Std;

UNSIGNED3 input_person_id := 100 : STORED('PersonID');
UNSIGNED4 input_num := 100 : STORED('Num');


F1 := FETCH($.DeclareData.Person.FilePlus_PersonID,
            $.DeclareData.IDX_Person_PersonID(PersonID<input_person_id),
						RIGHT.RecPos);

OUTPUT(CHOOSEN(F1, input_num));