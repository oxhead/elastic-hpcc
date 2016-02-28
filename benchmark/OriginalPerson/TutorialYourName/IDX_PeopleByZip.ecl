IMPORT TutorialYourName;
EXPORT IDX_PeopleByZIP :=
INDEX(TutorialYourName.File_TutorialPerson,{zip,fpos},'~tutorial::YN::PeopleByZipINDEX');
