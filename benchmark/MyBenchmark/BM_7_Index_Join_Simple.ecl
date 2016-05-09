IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
UNSIGNED4 input_num := 100 : STORED('Num');

idx1 := $.DeclareData.IDX__Person_LastName_FirstName;
idx1_filtered  := idx1(KEYED(input_last_name='' OR LastName=input_last_name),
										   KEYED(input_first_name='' OR FirstName=input_first_name));
f1 := FETCH(
    $.DeclareData.Person.FilePlus,
    idx1_filtered,
    RIGHT.RecPos,
		TRANSFORM($.DeclareData.Layout_Person_Simple, SELF := LEFT)
);

r1 := RECORD
 $.DeclareData.Layout_Person_Simple;
 $.DeclareData.Layout_Accounts;
END;
r2 := RECORD
 $.DeclareData.Layout_Person_Simple;
 UNSIGNED8 AcctRecPos;
END;
r2 Xform2($.DeclareData.Layout_Person_Simple L, $.DeclareData.IDX_Accounts_PersonID R) := TRANSFORM
	  SELF.AcctRecPos := R.RecPos;
    SELF := L;
END;
j2 := JOIN(
		CHOOSEN(f1, input_num),
		$.DeclareData.IDX_Accounts_PersonID,
		LEFT.PersonID=RIGHT.PersonID,
	  Xform2(LEFT,RIGHT),
		LIMIT(input_num)
); 


$.DeclareData.Layout_Person_Account_Simple Xform3($.DeclareData.Accounts L, r2 R) := TRANSFORM
 SELF := L;
 SELF := R;
END;
f2 := FETCH(
    $.DeclareData.Accounts,
    j2,
    RIGHT.AcctRecPos,
    Xform3(LEFT,RIGHT)
);

OUTPUT(COUNT(f2));
//OUTPUT(CHOOSEN(f2, input_num));
