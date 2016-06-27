IMPORT $;
IMPORT Std;

UNSIGNED3 input_person_id := 100 : STORED('PersonID');
UNSIGNED4 input_num := 100 : STORED('Num');
					
r1 := RECORD
  $.DeclareData.Layout_Person;
  $.DeclareData.Layout_Accounts;
END;

r1 Xform1($.DeclareData.Person_Sort.FilePlus_PersonID L,
          $.DeclareData.Accounts_Sort.FilePlus_PersonID R) := TRANSFORM
  SELF := L;
  SELF := R;
END;
						
J1 := JOIN($.DeclareData.Person_Sort.FilePlus_PersonID(PersonID<input_person_id),
           $.DeclareData.Accounts_Sort.FilePlus_PersonID,
					 LEFT.PersonID=RIGHT.PersonID,
           Xform1(LEFT,RIGHT),
           KEYED($.DeclareData.Accounts_Sort.IDX_PersonID));

OUTPUT(CHOOSEN(J1, input_num));