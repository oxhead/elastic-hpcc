Layout_Person := RECORD
  UNSIGNED3 PersonID;
  STRING15 FirstName;
  STRING25 LastName;
  STRING1   MiddleInitial;
  STRING1   Gender;
  STRING42 Street;
  STRING20 City;
  STRING2   State;
  STRING5  Zip;
END;

Layout_Accounts := RECORD
  STRING20 Account;
  STRING8  OpenDate;
  STRING2   IndustryCode;
  STRING1   AcctType;
  STRING1   AcctRate;
  UNSIGNED1 Code1;
  UNSIGNED1 Code2;
  UNSIGNED4 HighCredit;
  UNSIGNED4 Balance;
END;
Layout_Accounts_Link := RECORD
  UNSIGNED3 PersonID;
  Layout_Accounts;
END;
Layout_Combined := RECORD
  Layout_Person;
	DATASET(Layout_Accounts) Accounts;
END;


//write files to disk
D1 := DATASET('~PROGGUIDE::EXAMPLEDATA::People',{Layout_Person,UNSIGNED8 RecPos{VIRTUAL(fileposition)}}, THOR);
D2 := DATASET('~PROGGUIDE::EXAMPLEDATA::Accounts',{Layout_Accounts_Link,UNSIGNED8 RecPos{VIRTUAL(fileposition)}}, THOR);
D3 := DATASET('~PROGGUIDE::EXAMPLEDATA::PeopleAccts',{Layout_Combined,UNSIGNED8 RecPos{VIRTUAL(fileposition)}},THOR);


My_I1 := INDEX(D1,{LastName,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.LastName');
My_I2 := INDEX(D1,{FirstName,RecPos},'~PROGGUIDE::EXAMPLEDATA::KEYS::People.FirstName');
//My_B1 := BUILDINDEX(My_I1,OVERWRITE);
My_B2 := BUILDINDEX(My_I2,OVERWRITE);


My_P1 := PARALLEL(My_B2);

SEQUENTIAL(My_P1);