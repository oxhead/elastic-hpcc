IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
UNSIGNED4 input_num := 100 : STORED('Num');

idx1 := $.DeclareData.IDX__Person_LastName_FirstName;
idx1_filtered  := idx1(KEYED(input_last_name='' OR LastName=input_last_name) AND
										 KEYED(input_first_name='' OR FirstName=input_first_name));

f1 := FETCH(
    $.DeclareData.Person.FilePlus,
    idx1_filtered,
    RIGHT.RecPos
);		
ff1 := CHOOSEN(f1, input_num);	


r1 := RECORD
 $.DeclareData.Layout_Person;
 $.DeclareData.Layout_Accounts;
END;
r2 := RECORD
 $.DeclareData.Layout_Person;
 UNSIGNED8 AcctRecPos;
END;
r2 Xform2($.DeclareData.Person.FilePlus L, $.DeclareData.IDX_Accounts_PersonID R) := TRANSFORM
	  SELF.AcctRecPos := R.RecPos;
    SELF := L;
END;
j2 := JOIN(
    ff1,
 	  $.DeclareData.IDX_Accounts_PersonID,
	  LEFT.PersonID=RIGHT.PersonID,
	  Xform2(LEFT,RIGHT),
		LIMIT(input_num)
); 

r1 Xform3($.DeclareData.Accounts L, r2 R) := TRANSFORM
 SELF := L;
 SELF := R;
END;
f2 := FETCH(
    $.DeclareData.Accounts,
    j2,
    RIGHT.AcctRecPos,
    Xform3(LEFT,RIGHT)
);

OUTPUT(f2);