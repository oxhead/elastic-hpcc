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
base1_filtered := base1(input_last_name='' OR LastName=input_last_name,
											input_first_name='' OR FirstName=input_first_name,
											input_gender='' OR Gender=input_gender,
											input_city='' OR City=input_city,
											input_state='' OR State=input_state,
											input_zip='' OR Zip=input_zip);

base2 := $.DeclareData.Accounts;

$.DeclareData.Layout_Person_Account_Simple Xform1($.DeclareData.Accounts R, $.DeclareData.Person.FilePlus L) := TRANSFORM
    SELF := L;
    SELF := R;
END;

J1 := JOIN(base2,
           CHOOSEN(base1_filtered, input_num),
					 LEFT.PersonID=RIGHT.PersonID,
           Xform1(LEFT,RIGHT));
OUTPUT(CHOOSEN(J1, input_num));