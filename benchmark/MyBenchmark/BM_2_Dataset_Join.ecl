IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
STRING1 input_gender := '' : STORED('Gender');
STRING20 input_city := '' : STORED('City');
STRING2 input_state := '' : STORED('State');
STRING5 input_zip := '' : STORED('Zip');
UNSIGNED4 input_num := 100 : STORED('Num');


base1 := $.DeclareData.Person.FilePlus;
base1_filtered := base1(Std.Str.Contains(LastName, input_last_name, TRUE) AND
											Std.Str.Contains(FirstName, input_first_name, TRUE) AND
											Std.Str.Contains(Gender, input_gender, TRUE) AND
											Std.Str.Contains(City, input_city, TRUE) AND
											Std.Str.Contains(State, input_state, TRUE) AND
											Std.Str.Contains(Zip, input_zip, TRUE));

base2 := $.DeclareData.Accounts;

r1 := RECORD
    $.DeclareData.Layout_Person;
    $.DeclareData.Layout_Accounts;
END;


r1 Xform1($.DeclareData.Person.FilePlus L,
          $.DeclareData.Accounts R) := TRANSFORM
    SELF := L;
    SELF := R;
END;

J1 := JOIN(base1_filtered,
           base2,
					 LEFT.PersonID=RIGHT.PersonID,
           Xform1(LEFT,RIGHT),
					 LIMIT(input_num));
OUTPUT(J1);