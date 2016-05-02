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
base_filtered := base(input_last_name='' OR LastName=input_last_name,
											input_first_name='' OR FirstName=input_first_name,
											input_gender='' OR Gender=input_gender,
											input_city='' OR City=input_city,
											input_state='' OR State=input_state,
											input_zip='' OR Zip=input_zip);
//base_filtered := base(Std.Str.Contains(LastName, input_last_name, TRUE),
//											Std.Str.Contains(FirstName, input_first_name, TRUE),
//											Std.Str.Contains(Gender, input_gender, TRUE),
//											Std.Str.Contains(City, input_city, TRUE),
//											Std.Str.Contains(State, input_state, TRUE),
//											Std.Str.Contains(Zip, input_zip, TRUE));
											
o := PROJECT(CHOOSEN(base_filtered, input_num), TRANSFORM($.DeclareData.Layout_Person_Simple, SELF :=LEFT));
OUTPUT(o);
//OUTPUT(CHOOSEN(base_filtered, input_num));