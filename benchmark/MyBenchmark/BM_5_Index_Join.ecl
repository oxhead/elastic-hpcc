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
idx1_filtered  := idx1(KEYED(input_last_name='' OR LastName=input_last_name) AND
										 KEYED(input_first_name='' OR FirstName=input_first_name) AND
									   KEYED(input_gender='' OR Gender=input_gender) AND
										 KEYED(input_city='' OR City=input_city) AND
										 KEYED(input_state='' OR State=input_state) AND
										 KEYED(input_zip='' OR Zip=input_zip) AND
										 WILD(PersonID));

											
idx2 := $.DeclareData.IDX_Accounts_PersonID_Payload;											
	
r1 := RECORD
 $.DeclareData.IDX__Person_All_Payload ;
 $.DeclareData.IDX_Accounts_PersonID_Payload;
END;

r1 Xform1($.DeclareData.IDX__Person_All_Payload L,
          $.DeclareData.IDX_Accounts_PersonID_Payload R) := TRANSFORM
    SELF := L;
    SELF := R;
END;

j1 := JOIN(idx1_filtered,
					 idx2,
					 LEFT.PersonID=RIGHT.PersonID,
           Xform1(LEFT,RIGHT),
					 LIMIT(input_num)
); 

OUTPUT(j1);