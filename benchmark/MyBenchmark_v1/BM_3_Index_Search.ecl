IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
STRING1 input_gender := '' : STORED('Gender');
STRING20 input_city := '' : STORED('City');
STRING2 input_state := '' : STORED('State');
STRING5 input_zip := '' : STORED('Zip');
UNSIGNED4 input_num := 100 : STORED('Num');

idx := $.DeclareData.IDX__Person_All_Payload;
idx_filtered  := idx(KEYED(input_last_name='' OR LastName=input_last_name),
										 KEYED(input_first_name='' OR FirstName=input_first_name),
									   KEYED(input_gender='' OR Gender=input_gender),
										 KEYED(input_city='' OR City=input_city),
										 KEYED(input_state='' OR State=input_state),
										 KEYED(input_zip='' OR Zip=input_zip),
										 WILD(PersonID));
o := PROJECT(CHOOSEN(idx_filtered, input_num), TRANSFORM($.DeclareData.Layout_Person_Simple, SELF :=LEFT));
OUTPUT(o);
//OUTPUT(COUNT(idx_filtered));
//OUTPUT(CHOOSEN(idx_filtered, input_num));