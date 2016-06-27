IMPORT $;

UNSIGNED3 INDEX_END := 1000 :  STORED('index_end');
UNSIGNED3 INDEX_START := 1 :  STORED('index_start');

r1 := RECORD
 $.DeclareData.Layout_Person;
 $.DeclareData.Layout_Accounts;
END;
r2 := RECORD
 $.DeclareData.Layout_Person;
 UNSIGNED8 AcctRecPos;
END;
r2 Xform2($.DeclareData.Person.FilePlus L,
 $.DeclareData.IDX_Accounts_PersonID R) := TRANSFORM
 SELF.AcctRecPos := R.RecPos;
 SELF := L;
END;
J2 := JOIN($.DeclareData.Person.FilePlus(PersonID BETWEEN INDEX_START AND INDEX_END),
 $.DeclareData.IDX_Accounts_PersonID,
 LEFT.PersonID=RIGHT.PersonID,
 Xform2(LEFT,RIGHT));
r1 Xform3($.DeclareData.Accounts L, r2 R) := TRANSFORM
 SELF := L;
 SELF := R;
END;
F1 := FETCH($.DeclareData.Accounts,
 J2,
 RIGHT.AcctRecPos,
 Xform3(LEFT,RIGHT));
OUTPUT(F1,ALL);