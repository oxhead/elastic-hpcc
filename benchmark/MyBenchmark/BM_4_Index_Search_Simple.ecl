IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
UNSIGNED4 input_num := 100 : STORED('Num');


idx := $.DeclareData.IDX__Person_LastName_FirstName;
idx_filtered  := idx(KEYED(input_last_name='' OR LastName=input_last_name),
										 KEYED(input_first_name='' OR FirstName=input_first_name));

OUTPUT(CHOOSEN(idx_filtered, input_num));