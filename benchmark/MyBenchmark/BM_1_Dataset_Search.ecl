IMPORT $;
IMPORT Std;

STRING10 input_first_name := '' : STORED('FirstName');
STRING10 input_last_name := '' : STORED('LastName');
STRING1 input_gender := '' : STORED('Gender');
STRING20 input_city := '' : STORED('City');
STRING2 input_state := '' : STORED('State');
STRING5 input_zip := '' : STORED('Zip');
UNSIGNED4 input_num := 100 : STORED('Num');


base := $.DeclareData.Person.FilePlus;
base_filtered := base(Std.Str.Contains(LastName, input_last_name, TRUE) AND
											Std.Str.Contains(FirstName, input_first_name, TRUE) AND
											Std.Str.Contains(Gender, input_gender, TRUE) AND
											Std.Str.Contains(City, input_city, TRUE) AND
											Std.Str.Contains(State, input_state, TRUE) AND
											Std.Str.Contains(Zip, input_zip, TRUE));
OUTPUT(CHOOSEN(base_filtered, input_num));