IMPORT TutorialYourName;
STRING10 ZipFilter := '' :STORED('ZIPValue');
resultSet :=
 FETCH(TutorialYourName.File_TutorialPerson,
 TutorialYourName.IDX_PeopleByZIP(zip=ZipFilter),
 RIGHT.fpos);
OUTPUT(resultset);
