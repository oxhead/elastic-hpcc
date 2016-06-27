IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
STRING1 input_gender := '' : STORED('Gender');
STRING20 input_city := '' : STORED('City');
STRING2 input_state := '' : STORED('State');
STRING5 input_zip := '' : STORED('Zip');
UNSIGNED4 input_num := 100 : STORED('Num');

idx1 := $.DeclareData.IDX__Person_All_Payload;
idx1_filtered  := idx1(KEYED(input_last_name='' OR LastName=input_last_name),
										 KEYED(input_first_name='' OR FirstName=input_first_name),
									   KEYED(input_gender='' OR Gender=input_gender),
										 KEYED(input_city='' OR City=input_city),
										 KEYED(input_state='' OR State=input_state),
										 KEYED(input_zip='' OR Zip=input_zip),
										 WILD(PersonID)
										 );
											
idx2 := $.DeclareData.IDX_Accounts_PersonID_Payload;											
	
$.DeclareData.Layout_Person_Account_Simple Xform1($.DeclareData.IDX__Person_All_Payload L, $.DeclareData.IDX_Accounts_PersonID_Payload R) := TRANSFORM
    SELF := L;
    SELF := R;
END;


j1 := JOIN(CHOOSEN(idx1_filtered, input_num),
           idx2,
					 LEFT.PersonID=RIGHT.PersonID,
           Xform1(LEFT, RIGHT),
					 LIMIT(input_num)
); 

//OUTPUT(COUNT(j1));

OUTPUT(CHOOSEN(j1, input_num));