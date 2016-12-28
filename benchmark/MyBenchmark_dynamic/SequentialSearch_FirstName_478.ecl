IMPORT $;
IMPORT Std;

STRING15 input_firstname := 'MARY' : STORED('FirstName');
UNSIGNED4 input_num := 100 : STORED('Num');

EXPORT Layout_Person := RECORD
    UNSIGNED3 PersonID;
    STRING15  FirstName;
    STRING25  LastName;
    STRING1   MiddleInitial;
    STRING1   Gender;
    STRING42  Street;
    STRING20  City;
    STRING2   State;
    STRING5   Zip;
END;

ds := DATASET('~MyBenchmark::Data_Sorted_People_FirstName_478', {Layout_Person,UNSIGNED8 RecPos{virtual(fileposition)}}, THOR);
idx := INDEX(ds, {FirstName,RecPos}, '~MyBenchmark::Idx_Sorted_People_FirstName_478');

F1 := FETCH(ds, idx(FirstName=input_firstname), RIGHT.RecPos);
OUTPUT(CHOOSEN(F1,input_num));
